package predicates

import (
	v1 "k8s.io/api/core/v1"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

const (
	HAGroupLabel = "ha.app.group"
)

var (
	ErrHAGroupExistPod = newPredicateFailureError(HaGroupPred, "the node(s)  had pod with the same HAGroupLabel yet")
)

func HAGroupPredicates(pod *v1.Pod, meta PredicateMetadata, nodeInfo *schedulernodeinfo.NodeInfo) (bool, []PredicateFailureReason, error) {
	ha, ok := pod.Labels[HAGroupLabel]
	if !ok {
		return true, nil, nil
	}

	find := false
	for _, pod := range nodeInfo.Pods() {
		_ha, ok := pod.Labels[HAGroupLabel]
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
