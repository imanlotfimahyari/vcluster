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
	runner := flag.String("runtime-runner", "", "optional static runner for the managed-runtime image")
	api := flag.String("kube-apiserver", "", "static API server; required with --runtime-runner")
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
	type imageFile struct {
		name string
		mode int64
		data []byte
	}
	files := []imageFile{{"bin/busybox", 0755, content}, {"www/index.html", 0644, []byte("shared-etcd-pod-ok\n")}}
	tag := "docker.io/library/vcluster-test:local"
	if (*runner == "") != (*api == "") {
		panic("--runtime-runner and --kube-apiserver must be supplied together")
	}
	if *runner != "" {
		for _, item := range []struct{ name, path string }{{"test-runner", *runner}, {"binaries/kube-apiserver", *api}} {
			data, err := os.ReadFile(item.path)
			if err != nil {
				panic(err)
			}
			files = append(files, imageFile{item.name, 0755, data})
		}
		tag = "docker.io/library/vcluster-runtime-test:local"
	}
	for _, file := range files {
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
	// Containerd must be able to match the manifest to the offline guest.
	// empty.Image otherwise leaves these fields blank and skips unpacking.
	platform, err := image.ConfigFile()
	if err != nil {
		panic(err)
	}
	platform.OS, platform.Architecture = "linux", "amd64"
	image, err = mutate.ConfigFile(image, platform)
	if err != nil {
		panic(err)
	}
	ref, err := name.NewTag(tag)
	if err != nil {
		panic(err)
	}
	if err := tarball.WriteToFile(*output, ref, image); err != nil {
		panic(err)
	}
	fmt.Println("Offline workload image created")
}
