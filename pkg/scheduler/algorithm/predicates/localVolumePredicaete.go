package predicates

import (
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

var (
	ErrLocalVolumePressure          = newPredicateFailureError(CheckLocalDiskPred, "the node(s) local vg Pressure")
	PodLocalVolumeRequestAnnocation = "vg.localvolume.request"
)

type Resouce map[string]string

func parseJsonToResourceList(data string) (v1.ResourceList, error) {
	list := v1.ResourceList{}
	resoucereq := Resouce{}
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
	return cability, nil
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
		}

		for name, quantity := range reqresouce {
			countquantity, ok := counts[name]
			if !ok {
				counts[name] = quantity
				continue
			}
			countquantity.Add(quantity)
		}
	}
	return counts, nil
}

func LocalVolumePredicates(pod *v1.Pod, meta PredicateMetadata, nodeInfo *schedulernodeinfo.NodeInfo) (bool, []PredicateFailureReason, error) {
	request, ok := pod.Annotations[PodLocalVolumeRequestAnnocation]
	if !ok {
		return true, nil, nil
	}

	reqsouce, err := parseJsonToResourceList(request)
	if err != nil {
	}

	//获取主机总资源
	nodecability, err := getNodeCability(nodeInfo)
	if err != nil {
	}

	//统计已使用
	nodeused, err := countsNodeUsed(nodeInfo)
	if err != nil {
	}

	for name, quantity := range reqsouce {
		nodequantity, ok := nodeused[name]
		if !ok {
		}
		nodequantity.Add(quantity)
		nodeused[name] = nodequantity
	}

	//判断资源是否足够
	for name, quantity := range nodeused {
		_quantity, ok := nodecability[name]
		if !ok {
		}
		if _quantity.Cmp(quantity) < 0 {
		}
	}

	return true, nil, nil
}
