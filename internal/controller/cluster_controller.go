package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/filters"
	"github.com/Reactive-Network/backrest-operator/internal/metrics"
)

const (
	defaultBackrestImage = "ghcr.io/garethgeorge/backrest"
	defaultBackrestTag   = "v1.14.1"
	backrestPort         = int32(9898)
)

type BackrestClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *BackrestClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("backrestcluster", req.NamespacedName)
	var cluster operatorv1alpha1.BackrestCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !filters.ObjectAllowed(cluster.Namespace, cluster.Labels) {
		logger.V(1).Info("skipped by watch filter")
		return ctrl.Result{}, nil
	}

	if err := r.ensureHost(ctx, &cluster); err != nil {
		metrics.ReconcileErrors.WithLabelValues("BackrestCluster").Inc()
		logger.Error(err, "ensure host failed")
		return r.patchStatus(ctx, &cluster, "Failed", false, 0, 0, 0)
	}
	agentsReady, agentsDesired, err := r.ensureAgents(ctx, &cluster)
	if err != nil {
		metrics.ReconcileErrors.WithLabelValues("BackrestCluster").Inc()
		logger.Error(err, "ensure agents failed")
		return r.patchStatus(ctx, &cluster, "Failed", false, agentsReady, agentsDesired, 0)
	}
	hostReady := r.isHostReady(ctx, &cluster)
	phase := "Pending"
	if hostReady && agentsReady >= agentsDesired {
		phase = "Ready"
	} else if hostReady && agentsDesired > 0 && agentsReady < agentsDesired {
		phase = "Degraded"
	}
	paired := int32(0)
	if hostReady {
		paired = agentsReady
	}
	logger.V(1).Info("reconciled", "phase", phase, "hostReady", hostReady, "agentsReady", agentsReady, "agentsDesired", agentsDesired)
	if phase == "Degraded" || phase == "Failed" {
		logger.Info("cluster not ready", "phase", phase, "hostReady", hostReady, "agents", fmt.Sprintf("%d/%d", agentsReady, agentsDesired))
	}
	// A host pod restart also changes the owned Deployment, so this runs right after the new
	// Backrest comes up with its default config (auth disabled, no repositories).
	var requeue time.Duration
	if hostReady {
		if err := syncClusterHost(ctx, r.Client, &cluster); err != nil {
			metrics.ReconcileErrors.WithLabelValues("BackrestCluster").Inc()
			logger.Error(err, "sync Backrest host config")
			requeue = hostSyncRetry
		} else if cluster.Spec.Auth.Enabled {
			requeue = hostAuthResync
		}
	}
	res, err := r.patchStatus(ctx, &cluster, phase, hostReady, agentsReady, agentsDesired, paired)
	if err == nil && requeue > 0 {
		res.RequeueAfter = requeue
	}
	return res, err
}

const (
	// hostSyncRetry is the retry delay after a failed host config sync.
	hostSyncRetry = 30 * time.Second
	// hostAuthResync re-checks the auth block while auth is enabled, in case the host config
	// was changed by hand or the host restarted without an event reaching the operator.
	hostAuthResync = 5 * time.Minute
	// annRestartedAt is set on the pod template by `kubectl rollout restart`.
	annRestartedAt = "kubectl.kubernetes.io/restartedAt"
)

// keepRestartedAt copies the rollout-restart annotation from the live pod template into the
// desired one. Without it the operator's next template update drops the annotation, which rolls
// the host (and its config) or every agent a second time.
func keepRestartedAt(cur, desired *corev1.PodTemplateSpec) {
	v, ok := cur.Annotations[annRestartedAt]
	if !ok {
		return
	}
	if desired.Annotations == nil {
		desired.Annotations = map[string]string{}
	}
	if _, set := desired.Annotations[annRestartedAt]; !set {
		desired.Annotations[annRestartedAt] = v
	}
}

func (r *BackrestClusterReconciler) patchStatus(ctx context.Context, c *operatorv1alpha1.BackrestCluster, phase string, hostReady bool, ready, desired, paired int32) (ctrl.Result, error) {
	c.Status.Phase = phase
	c.Status.HostReady = hostReady
	c.Status.AgentsReady = ready
	c.Status.AgentsDesired = desired
	c.Status.MultihostPaired = paired
	return ctrl.Result{}, r.Status().Update(ctx, c)
}

func imageFor(spec operatorv1alpha1.BackrestClusterSpec) string {
	img := spec.Image
	if img == "" {
		img = defaultBackrestImage
	}
	ver := spec.Version
	if ver == "" {
		ver = defaultBackrestTag
	}
	// if image already has tag, keep it
	for i := len(img) - 1; i >= 0; i-- {
		if img[i] == '/' {
			break
		}
		if img[i] == ':' {
			return img
		}
	}
	return img + ":" + ver
}

func (r *BackrestClusterReconciler) ensureHost(ctx context.Context, c *operatorv1alpha1.BackrestCluster) error {
	name, ns := c.Name, c.Namespace
	hostName := "backrest-host-" + name
	svcName := hostName
	pvcName := "backrest-host-data-" + name
	labels := map[string]string{
		"app.kubernetes.io/name":       "backrest",
		"app.kubernetes.io/instance":   name,
		"app.kubernetes.io/component":  "host",
		"app.kubernetes.io/managed-by": "backrest-operator",
		"operator.backrest.io/cluster": name,
		"operator.backrest.io/role":    "host",
	}
	size := c.Spec.Host.Persistence.Size
	if size == "" {
		size = "20Gi"
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: ns, Labels: labels},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
	if sc := c.Spec.Host.Persistence.StorageClassName; sc != "" {
		pvc.Spec.StorageClassName = &sc
	}
	_ = controllerutil.SetControllerReference(c, pvc, r.Scheme)
	if err := r.createOrIgnore(ctx, pvc); err != nil {
		return err
	}

	replicas := int32(1)
	if c.Spec.Host.Replicas != nil {
		replicas = *c.Spec.Host.Replicas
	}
	enableLinks := false
	if c.Spec.Host.EnableServiceLinks != nil {
		enableLinks = *c.Spec.Host.EnableServiceLinks
	}
	serverURL := c.Spec.Agents.Multihost.ServerURL
	if serverURL == "" {
		serverURL = fmt.Sprintf("http://%s.%s.svc:%d", svcName, ns, backrestPort)
	}
	img := imageFor(c.Spec)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: hostName, Namespace: ns, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					EnableServiceLinks: &enableLinks,
					Containers: []corev1.Container{{
						Name:  "backrest",
						Image: img,
						Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: backrestPort}},
						Env: []corev1.EnvVar{
							{Name: "BACKREST_PORT", Value: fmt.Sprintf(":%d", backrestPort)},
							{Name: "BACKREST_DATA", Value: "/data"},
							{Name: "BACKREST_MULTIHOST_SERVER_URL", Value: serverURL},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(backrestPort)}},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
						},
					}},
					NodeSelector: c.Spec.Host.NodeSelector,
				},
			},
		},
	}
	_ = controllerutil.SetControllerReference(c, dep, r.Scheme)
	if err := r.createOrUpdateDep(ctx, dep); err != nil {
		return err
	}

	svcType := corev1.ServiceTypeClusterIP
	if c.Spec.Host.ServiceType != "" {
		svcType = corev1.ServiceType(c.Spec.Host.ServiceType)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: ns, Labels: labels},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: labels,
			Ports:    []corev1.ServicePort{{Name: "http", Port: backrestPort, TargetPort: intstr.FromInt32(backrestPort)}},
		},
	}
	_ = controllerutil.SetControllerReference(c, svc, r.Scheme)
	if err := r.createOrIgnore(ctx, svc); err != nil {
		return err
	}
	return r.ensureIngress(ctx, c, svcName, labels)
}

func (r *BackrestClusterReconciler) ensureIngress(ctx context.Context, c *operatorv1alpha1.BackrestCluster, svcName string, labels map[string]string) error {
	ingName := "backrest-host-" + c.Name
	if !c.Spec.Host.Ingress.Enabled {
		_ = r.Delete(ctx, &netv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: ingName, Namespace: c.Namespace}})
		_ = r.Delete(ctx, &netv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: ingName + "-download", Namespace: c.Namespace}})
		return nil
	}
	host := c.Spec.Host.Ingress.Host
	if host == "" {
		host = "backrest.example.com"
	}
	backendName := svcName
	if c.Spec.Host.Ingress.BackendServiceName != "" {
		backendName = c.Spec.Host.Ingress.BackendServiceName
	}
	backendPort := int32(backrestPort)
	if c.Spec.Host.Ingress.BackendServicePort != 0 {
		backendPort = c.Spec.Host.Ingress.BackendServicePort
	}
	pathType := netv1.PathTypePrefix
	ing := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        ingName,
			Namespace:   c.Namespace,
			Labels:      labels,
			Annotations: c.Spec.Host.Ingress.Annotations,
		},
		Spec: netv1.IngressSpec{
			Rules: []netv1.IngressRule{{
				Host: host,
				IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{
					Paths: []netv1.HTTPIngressPath{{
						Path: "/", PathType: &pathType,
						Backend: netv1.IngressBackend{Service: &netv1.IngressServiceBackend{
							Name: backendName, Port: netv1.ServiceBackendPort{Number: backendPort},
						}},
					}},
				}},
			}},
		},
	}
	if c.Spec.Host.Ingress.ClassName != "" {
		ing.Spec.IngressClassName = &c.Spec.Host.Ingress.ClassName
	}
	if len(c.Spec.Host.Ingress.TLS) > 0 {
		b, err := jsonMarshal(c.Spec.Host.Ingress.TLS)
		if err == nil {
			var tls []netv1.IngressTLS
			if jsonUnmarshal(b, &tls) == nil {
				ing.Spec.TLS = tls
			}
		}
	}
	_ = controllerutil.SetControllerReference(c, ing, r.Scheme)
	if err := r.createOrUpdateIngress(ctx, ing); err != nil {
		return err
	}
	return r.ensureDownloadIngress(ctx, c, svcName, labels, host)
}

func (r *BackrestClusterReconciler) ensureDownloadIngress(ctx context.Context, c *operatorv1alpha1.BackrestCluster, svcName string, labels map[string]string, host string) error {
	ingName := "backrest-host-" + c.Name + "-download"
	bypass := c.Spec.Host.Ingress.DownloadBypass
	enabled := c.Spec.Host.Ingress.Enabled
	if bypass.Enabled != nil {
		enabled = *bypass.Enabled && c.Spec.Host.Ingress.Enabled
	}
	if !enabled {
		_ = r.Delete(ctx, &netv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: ingName, Namespace: c.Namespace}})
		return nil
	}
	path := bypass.Path
	if path == "" {
		path = "/download"
	}
	pathType := netv1.PathTypePrefix
	ann := map[string]string{}
	for k, v := range c.Spec.Host.Ingress.Annotations {
		ann[k] = v
	}
	// Prefer this path over a catch-all UI Ingress that fronts oauth2-proxy.
	if _, ok := ann["traefik.ingress.kubernetes.io/router.priority"]; !ok {
		ann["traefik.ingress.kubernetes.io/router.priority"] = "100"
	}
	for k, v := range bypass.Annotations {
		ann[k] = v
	}
	dlLabels := map[string]string{}
	for k, v := range labels {
		dlLabels[k] = v
	}
	dlLabels["app.kubernetes.io/component"] = "download"
	ing := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        ingName,
			Namespace:   c.Namespace,
			Labels:      dlLabels,
			Annotations: ann,
		},
		Spec: netv1.IngressSpec{
			Rules: []netv1.IngressRule{{
				Host: host,
				IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{
					Paths: []netv1.HTTPIngressPath{{
						Path: path, PathType: &pathType,
						Backend: netv1.IngressBackend{Service: &netv1.IngressServiceBackend{
							Name: svcName, Port: netv1.ServiceBackendPort{Number: backrestPort},
						}},
					}},
				}},
			}},
		},
	}
	if c.Spec.Host.Ingress.ClassName != "" {
		ing.Spec.IngressClassName = &c.Spec.Host.Ingress.ClassName
	}
	if len(c.Spec.Host.Ingress.TLS) > 0 {
		b, err := jsonMarshal(c.Spec.Host.Ingress.TLS)
		if err == nil {
			var tls []netv1.IngressTLS
			if jsonUnmarshal(b, &tls) == nil {
				ing.Spec.TLS = tls
			}
		}
	}
	_ = controllerutil.SetControllerReference(c, ing, r.Scheme)
	return r.createOrUpdateIngress(ctx, ing)
}

func (r *BackrestClusterReconciler) ensureAgents(ctx context.Context, c *operatorv1alpha1.BackrestCluster) (ready, desired int32, err error) {
	enabled := true
	if c.Spec.Agents.Enabled != nil {
		enabled = *c.Spec.Agents.Enabled
	}
	if !enabled {
		return 0, 0, nil
	}
	name, ns := c.Name, c.Namespace
	dsName := "backrest-agent-" + name
	labels := map[string]string{
		"app.kubernetes.io/name":       "backrest",
		"app.kubernetes.io/instance":   name,
		"app.kubernetes.io/component":  "agent",
		"app.kubernetes.io/managed-by": "backrest-operator",
		"operator.backrest.io/cluster": name,
		"operator.backrest.io/role":    "agent",
	}
	serverURL := c.Spec.Agents.Multihost.ServerURL
	if serverURL == "" {
		serverURL = fmt.Sprintf("http://backrest-host-%s.%s.svc:%d", name, ns, backrestPort)
	}
	enableLinks := false
	podSpec := corev1.PodSpec{
		EnableServiceLinks: &enableLinks,
		Containers: []corev1.Container{{
			Name:  "backrest",
			Image: imageFor(c.Spec),
			Env: []corev1.EnvVar{
				{Name: "BACKREST_PORT", Value: fmt.Sprintf(":%d", backrestPort)},
				{Name: "BACKREST_DATA", Value: "/data"},
				{Name: "BACKREST_MULTIHOST_SERVER_URL", Value: serverURL},
			},
			VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
		}},
		Volumes:      []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		NodeSelector: c.Spec.Agents.NodeSelector,
		Tolerations:  mapsToTolerations(c.Spec.Agents.Tolerations),
	}
	mode := c.Spec.Agents.Mode
	if mode == "" {
		mode = "DaemonSet"
	}
	if mode == "Deployment" {
		replicas := int32(1)
		if c.Spec.Agents.Replicas != nil {
			replicas = *c.Spec.Agents.Replicas
		}
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: dsName, Namespace: ns, Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: podSpec},
			},
		}
		_ = controllerutil.SetControllerReference(c, dep, r.Scheme)
		if err := r.createOrUpdateDep(ctx, dep); err != nil {
			return 0, replicas, err
		}
		var cur appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Name: dsName, Namespace: ns}, &cur); err == nil {
			if cur.Status.ReadyReplicas > 0 {
				ready = cur.Status.ReadyReplicas
			}
			if cur.Spec.Replicas != nil {
				desired = *cur.Spec.Replicas
			}
		}
		return ready, desired, nil
	}
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: dsName, Namespace: ns, Labels: labels},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: podSpec},
		},
	}
	_ = controllerutil.SetControllerReference(c, ds, r.Scheme)
	if err := r.createOrUpdateDS(ctx, ds); err != nil {
		return 0, 0, err
	}
	var cur appsv1.DaemonSet
	if err := r.Get(ctx, types.NamespacedName{Name: dsName, Namespace: ns}, &cur); err == nil {
		ready = cur.Status.NumberReady
		desired = cur.Status.DesiredNumberScheduled
	}
	return ready, desired, nil
}

func (r *BackrestClusterReconciler) isHostReady(ctx context.Context, c *operatorv1alpha1.BackrestCluster) bool {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Name: "backrest-host-" + c.Name, Namespace: c.Namespace}, &dep); err != nil {
		return false
	}
	return dep.Status.ReadyReplicas >= 1
}

func (r *BackrestClusterReconciler) createOrIgnore(ctx context.Context, obj client.Object) error {
	err := r.Create(ctx, obj)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func mapsToTolerations(in []map[string]interface{}) []corev1.Toleration {
	if len(in) == 0 {
		return nil
	}
	b, err := jsonMarshal(in)
	if err != nil {
		return nil
	}
	var out []corev1.Toleration
	if err := jsonUnmarshal(b, &out); err != nil {
		return nil
	}
	return out
}

func (r *BackrestClusterReconciler) createOrUpdateDep(ctx context.Context, desired *appsv1.Deployment) error {
	var cur appsv1.Deployment
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &cur)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	// Deployment.spec.selector is immutable — keep existing selector/template labels in sync.
	if cur.Spec.Selector != nil {
		desired.Spec.Selector = cur.Spec.Selector
		if cur.Spec.Selector.MatchLabels != nil {
			desired.Spec.Template.Labels = cur.Spec.Selector.MatchLabels
		}
	}
	keepRestartedAt(&cur.Spec.Template, &desired.Spec.Template)
	cur.Spec.Replicas = desired.Spec.Replicas
	cur.Spec.Strategy = desired.Spec.Strategy
	cur.Spec.Template = desired.Spec.Template
	if desired.Labels != nil {
		cur.Labels = desired.Labels
	}
	return r.Update(ctx, &cur)
}

func (r *BackrestClusterReconciler) createOrUpdateDS(ctx context.Context, desired *appsv1.DaemonSet) error {
	var cur appsv1.DaemonSet
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &cur)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if cur.Spec.Selector != nil {
		desired.Spec.Selector = cur.Spec.Selector
		if cur.Spec.Selector.MatchLabels != nil {
			desired.Spec.Template.Labels = cur.Spec.Selector.MatchLabels
		}
	}
	keepRestartedAt(&cur.Spec.Template, &desired.Spec.Template)
	cur.Spec.Template = desired.Spec.Template
	if desired.Labels != nil {
		cur.Labels = desired.Labels
	}
	return r.Update(ctx, &cur)
}

func (r *BackrestClusterReconciler) createOrUpdateIngress(ctx context.Context, desired *netv1.Ingress) error {
	var cur netv1.Ingress
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &cur)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	cur.Spec = desired.Spec
	cur.Annotations = desired.Annotations
	if desired.Labels != nil {
		cur.Labels = desired.Labels
	}
	return r.Update(ctx, &cur)
}

func (r *BackrestClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.BackrestCluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.DaemonSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
