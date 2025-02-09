package main

import (
	"context"
	"fmt"
	"io"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"net/http"
	"os"
)

// DownloadYAML fetches the YAML file from a GitHub raw URL and returns its content as a byte slice
func DownloadYAML(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to download YAML: %v", err)
	}
	defer func(Body io.ReadCloser) {
		er := Body.Close()
		if er != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch YAML, status code: %d", resp.StatusCode)
	}

	// Read the response body
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read YAML content: %v", err)
	}

	return data, nil
}

// ApplyYAML applies a YAML manifest to the Kubernetes cluster
func ApplyYAML(yamlContent []byte) error {
	// Create Kubernetes config
	config, err := rest.InClusterConfig() // Use incluster config (inside a pod)
	if err != nil {
		config, err = rest.InClusterConfig() // Use default kubeconfig if running locally
		if err != nil {
			return fmt.Errorf("failed to get Kubernetes config: %v", err)
		}
	}

	// Create dynamic client
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	// Create RESTMapper to determine the GroupVersionResource
	discoveryClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %v", err)
	}

	// Parse the YAML content into runtime objects
	decoder := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	obj := &unstructured.Unstructured{}
	_, gvk, err := decoder.Decode(yamlContent, nil, obj)
	if err != nil {
		return fmt.Errorf("failed to decode YAML: %v", err)
	}

	// Discover REST mappings
	gr, err := restmapper.GetAPIGroupResources(discoveryClient)
	if err != nil {
		return fmt.Errorf("failed to get API group resources: %v", err)
	}
	mapper := restmapper.NewDiscoveryRESTMapper(gr)
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("failed to get REST mapping: %v", err)
	}

	// Create or update the resource
	resourceClient := dynamicClient.Resource(mapping.Resource).Namespace(obj.GetNamespace())
	_, err = resourceClient.Get(context.TODO(), obj.GetName(), metaV1.GetOptions{})
	if err == nil {
		// Resource exists, update it
		_, err = resourceClient.Update(context.TODO(), obj, metaV1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update resource: %v", err)
		}
		log.Println("Resource updated successfully")
	} else {
		// Resource doesn't exist, create it
		_, err = resourceClient.Create(context.TODO(), obj, metaV1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create resource: %v", err)
		}
		log.Println("Resource created successfully")
	}

	return nil
}

func main() {
	// Example raw GitHub URL (Replace with your actual file URL)
	yamlURL := "https://raw.githubusercontent.com/shishir9159/vanguard-fiesta/main/content/kubearmor.yaml"

	yamlData, err := DownloadYAML(yamlURL)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// Print YAML content (optional)
	fmt.Println("Downloaded YAML content:")
	fmt.Println(string(yamlData))

}
