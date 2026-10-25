// Build the offline BusyBox workload image without a container daemon.
package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

func main() {
	binary := flag.String("busybox", "", "path to a static Linux BusyBox binary")
	output := flag.String("output", "", "output image tar")
	flag.Parse()
	if *binary == "" || *output == "" {
		panic("--busybox and --output are required")
	}
	content, err := os.ReadFile(*binary)
	if err != nil {
		panic(err)
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, file := range []struct {
		name string
		mode int64
		data []byte
	}{{"bin/busybox", 0755, content}, {"www/index.html", 0644, []byte("shared-etcd-pod-ok\n")}} {
		if err := writer.WriteHeader(&tar.Header{Name: file.name, Mode: file.mode, Size: int64(len(file.data))}); err != nil {
			panic(err)
		}
		if _, err := writer.Write(file.data); err != nil {
			panic(err)
		}
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	layer, err := tarball.LayerFromReader(bytes.NewReader(archive.Bytes()))
	if err != nil {
		panic(err)
	}
	image, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		panic(err)
	}
	ref, err := name.NewTag("docker.io/library/vcluster-test:local")
	if err != nil {
		panic(err)
	}
	if err := tarball.WriteToFile(*output, ref, image); err != nil {
		panic(err)
	}
	fmt.Println("Offline workload image created")
}
