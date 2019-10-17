package main

import (
	"context"
	"flag"
	"fmt"
	"k8s.io/client-go/rest"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)

	var endpointLockName string
	var endpointLockNamespace string
	var id string

	flag.StringVar(&id, "id", "", "the holder identity name")
	flag.StringVar(&endpointLockName, "lease-lock-name", "example", "the lease lock resource name")
	flag.StringVar(&endpointLockNamespace, "lease-lock-namespace", "nicktming", "the lease lock resource namespace")
	flag.Parse()

	if id == "" {
		fmt.Printf("unable to get id (missing id flag).\n")
	}

	config := &rest.Config{
		Host: "http://172.21.0.16:8080",
	}
	client := clientset.NewForConfigOrDie(config)

	lock := &resourcelock.EndpointsLock{
		EndpointsMeta: metav1.ObjectMeta{
			Name:      endpointLockName,
			Namespace: endpointLockNamespace,
		},
		Client: client.CoreV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	// use a Go context so we can tell the leaderelection code when we
	// want to step down
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// use a client that will stop allowing new requests once the context ends
	//	config.Wrap(transport.ContextCanceller(ctx, fmt.Errorf("the leader is shutting down")))

	// listen for interrupts or the Linux SIGTERM signal and cancel
	// our context, which the leader election code will observe and
	// step down
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		fmt.Printf("Received termination, signaling shutdown\n")
		cancel()
	}()

	// start the leader election code loop
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock,
		// IMPORTANT: you MUST ensure that any code you have that
		// is protected by the lease must terminate **before**
		// you call cancel. Otherwise, you could have a background
		// loop still running and another process could
		// get elected before your background loop finished, violating
		// the stated goal of the lease.
		//		ReleaseOnCancel: true,
		LeaseDuration:   60 * time.Second,
		RenewDeadline:   15 * time.Second,
		RetryPeriod:     5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// we're notified when we start - this is where you would
				// usually put your code
				fmt.Printf("%s: leading\n", id)
			},
			OnStoppedLeading: func() {
				// we can do cleanup here, or after the RunOrDie method
				// returns
				fmt.Printf("%s: lost\n", id)
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if identity == id {
					// I just got the lock
					return
				}
				fmt.Printf("new leader elected: %v\n", identity)
			},
		},
	})

	// because the context is closed, the client should report errors
	_, err := client.CoreV1().Endpoints(endpointLockNamespace).Get(endpointLockName, metav1.GetOptions{})
	if err == nil || !strings.Contains(err.Error(), "the leader is shutting down") {
		fmt.Printf("%s: expected to get an error when trying to make a client call: %v\n", id, err)
		return
	}

	// we no longer hold the lease, so perform any cleanup and then
	// exit
	fmt.Printf("%s: done", id)
}