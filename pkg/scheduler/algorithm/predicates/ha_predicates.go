package predicates

import (
	v1 "k8s.io/api/core/v1"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

const (
	HostHAGroupLabel = "ha.host.group"
)

var (
	ErrHAGroupExistPod = newPredicateFailureError(HostHaGroupPred, "the node(s)  had pod with the same HAGroupLabel yet")
)

func HAGroupPredicates(pod *v1.Pod, meta PredicateMetadata, nodeInfo *schedulernodeinfo.NodeInfo) (bool, []PredicateFailureReason, error) {
	ha, ok := pod.Labels[HostHAGroupLabel]
	if !ok {
		return true, nil, nil
	}

	find := false
	for _, pod := range nodeInfo.Pods() {
		_ha, ok := pod.Labels[HostHAGroupLabel]
		if !ok {
			continue
		}
		if _ha == ha {
			find = true
			break
		}
	}

	if find {
		return false, []PredicateFailureReason{ErrHAGroupExistPod}, nil
	}

	return true, nil, nil

}
