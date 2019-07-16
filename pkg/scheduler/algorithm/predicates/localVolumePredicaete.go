package predicates

import (
	"encoding/json"
	"fmt"

	// "k8s.io/klog"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

var (
	ErrLocalVolumePressure = newPredicateFailureError(CheckLocalVolumePred, "the node(s) local vg Pressure")
	ErrLocalVolumeNotExit  = newPredicateFailureError(CheckLocalVolumePred, "the node(s) not has related resouce")

	PodLocalVolumeRequestAnnocation = "vg.localvolume.request"
	NodeLocalVolumeAnnocation       = "vg.localvolume.cability"
)

func LocalVolumePredicates(pod *v1.Pod, meta PredicateMetadata, nodeInfo *schedulernodeinfo.NodeInfo) (bool, []PredicateFailureReason, error) {
	requeststr, ok := pod.Annotations[PodLocalVolumeRequestAnnocation]
	if !ok {
		return true, nil, nil
	}

	requestsource, err := parseJsonToResourceList(requeststr)
	if err != nil {
		return false, nil, fmt.Errorf(" parse pod(%s) request fail:%s(data:%s)", pod.GetName(), err.Error(), requeststr)
	}

	//获取主机总资源
	nodecability, err := getNodeCability(nodeInfo)
	if err != nil {
		return false, nil, fmt.Errorf(" %s:getNodeCability fail:%s", nodeInfo.Node().GetName(), err.Error())
	}

	//统计已使用
	nodeused, err := countsNodeUsed(nodeInfo)
	if err != nil {
		return false, nil, fmt.Errorf(" %s:countsNodeUsed fail:%s", nodeInfo.Node().GetName(), err.Error())
	}

	for name, quantity := range requestsource {
		nodequantity, ok := nodeused[name]
		if !ok {
			nodeused[name] = quantity
			continue
		}

		nodequantity.Add(quantity)
		nodeused[name] = nodequantity
	}

	//判断资源是否足够
	for name, _ := range requestsource {
		uesdquantity, ok := nodeused[name]
		if !ok {
			return false, []PredicateFailureReason{ErrLocalVolumeNotExit}, nil // fmt.Errorf("%s:node not have %s type resouce", nodeInfo.Node().GetName(), name)
		}

		cabilityquantity, ok := nodecability[name]
		if !ok {
			return false, []PredicateFailureReason{ErrLocalVolumeNotExit}, nil // fmt.Errorf("%s:node not have %s type resouce", nodeInfo.Node().GetName(), name)
		}

		if cabilityquantity.Cmp(uesdquantity) < 0 {
			return false, []PredicateFailureReason{ErrLocalVolumePressure}, nil
		}
	}

	return true, nil, nil
}

func parseJsonToResourceList(data string) (v1.ResourceList, error) {
	list := v1.ResourceList{}
	resoucereq := map[string]string{}
	err := json.Unmarshal([]byte(data), &resoucereq)
	if err != nil {
		return list, err
	}

	for vg, valueStr := range resoucereq {
		value, err := resource.ParseQuantity(valueStr)
		if err != nil {
			return list, err
		}
		list[v1.ResourceName(vg)] = value
	}

	return list, nil
}

func getNodeCability(nodeInfo *schedulernodeinfo.NodeInfo) (v1.ResourceList, error) {
	cability := v1.ResourceList{}
	cabilitystr, ok := nodeInfo.Node().Annotations[NodeLocalVolumeAnnocation]
	if !ok {
		return cability, nil
	}
	return parseJsonToResourceList(cabilitystr)
}

func countsNodeUsed(nodeInfo *schedulernodeinfo.NodeInfo) (v1.ResourceList, error) {
	counts := v1.ResourceList{}
	for _, pod := range nodeInfo.Pods() {
		req, ok := pod.Annotations[PodLocalVolumeRequestAnnocation]
		if !ok {
			continue
		}

		reqresouce, err := parseJsonToResourceList(req)
		if err != nil {
			return counts, err
		}

		for name, quantity := range reqresouce {
			countquantity, ok := counts[name]
			if !ok {
				counts[name] = quantity
				continue
			}
			countquantity.Add(quantity)
			counts[name] = countquantity
		}
	}
	return counts, nil
}
