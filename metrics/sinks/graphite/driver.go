package graphite

import (
	"fmt"
	graphite_client "github.com/marpaia/graphite-golang"
	"k8s.io/heapster/metrics/core"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type GraphiteSink struct {
	client *graphite_client.Graphite
	sync.RWMutex
}

func (sink *GraphiteSink) ExportData(dataBatch *core.DataBatch) {
	sink.Lock()
	sink.Unlock()
	var series []graphite_client.Metric
	var metricValue string
	var metricName string
	for _, metricSet := range dataBatch.MetricSets {
		metricName = ""
		labels := metricSet.Labels
		if labels["namespace_name"] == "chaos" {
			continue
		}
		switch labels["type"] {
		case core.MetricSetTypeNode:
			metricName = metricName + core.MetricSetTypeNode + "." + labels["nodename"]
		case core.MetricSetTypePod:
			metricName = metricName + core.MetricSetTypePod + "." + labels["pod_name"]
		case core.MetricSetTypePodContainer:
			metricName = metricName + core.MetricSetTypePodContainer + "." + labels["container_name"]
		case core.MetricSetTypeNamespace:
			metricName = metricName + core.MetricSetTypeNamespace + "." + labels["namespace_name"]
		case core.MetricSetTypeCluster:
			metricName = metricName + core.MetricSetTypeCluster
		case core.MetricSetTypeSystemContainer:
			metricName = metricName + core.MetricSetTypeSystemContainer
		}
		for name, value := range metricSet.MetricValues {
			ametricName := metricName + "." + name
			switch value.ValueType {
			case core.ValueInt64:
				metricValue = strconv.FormatInt(int64(value.IntValue), 10)
			case core.ValueFloat:
				metricValue = strconv.FormatFloat(float64(value.FloatValue), 'E', -1, 32)
			}
			ametricName = strings.Replace(ametricName, "/", ".", -1)
			ametricName = strings.Replace(ametricName, "..", ".", -1)
			//fmt.Println(ametricName,metricValue, time.Now())
			series = append(series, graphite_client.Metric{Name: ametricName, Value: metricValue, Timestamp: time.Now().Unix()})
		}
		for _, metric := range metricSet.LabeledMetrics {
			labeledValue := metric.GetValue()
			switch labeledValue.(type) {
			case float32:
				alabeledValue, _ := labeledValue.(float32)
				metricValue = strconv.FormatFloat(float64(alabeledValue), 'E', -1, 32)
			case int64:
				alabeledValue, _ := labeledValue.(int64)
				metricValue = strconv.FormatInt(int64(alabeledValue), 10)
			}
			mName := fmt.Sprintf("%s.%s.%s", metricName, metric.Name, metric.Labels["resource_id"])
			mName = strings.Replace(mName, "/", ".", -1)
			mName = strings.Replace(mName, "..", ".", -1)
			//fmt.Println(mName,metricValue, time.Now())
			series = append(series, graphite_client.Metric{Name: mName, Value: metricValue, Timestamp: time.Now().Unix()})
		}
	}
	sink.sendGraphiteMetrics(series)
}

func (sink *GraphiteSink) sendGraphiteMetrics(series []graphite_client.Metric) error {
	length := len(series)
	start := 0
	end := 10
	for {
		err := sink.client.SendMetrics(series[start:end])
                if err != nil {
			fmt.Println(err)
		}
		start = start + 10
		end = end + 10
		if end >= length {
			sink.client.SendMetrics(series[start:])
			break
		}
	}
	return nil
}

func (sink *GraphiteSink) Name() string {
	return "Graphite Sink"
}

func (sink *GraphiteSink) Stop() {}
func NewGraphiteSink(url *url.URL) (core.DataSink, error) {
	fmt.Println("NewGraphiteSink")
	hostport := strings.Split(url.Host, ":")
	port, err := strconv.Atoi(hostport[1])
	if err != nil {
		return &GraphiteSink{}, err
	}
	client, err := graphite_client.NewGraphiteUDP(hostport[0], port, "k8s")
	return &GraphiteSink{client: client}, nil
}
