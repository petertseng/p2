package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/util/net"
	"github.com/square/p2/pkg/version"
)

var (
	replicate = kingpin.New("p2-mini-orchestrator", `p2-mini-orchestrator uses the rc package to schedule deployment of a pod across multiple nodes. See the rc package's README and godoc for more information.

	Example invocation: p2-mini-orchestrator --nodes 2 --node-endpoint=https://example.com/api/nodes/select --selector=app=helloworld /tmp/helloworld.yaml

	This will take the pod whose manifest is located at /tmp/helloworld.yaml and
	deploy it to nodes that match the label selector "app=helloworld", querying
	https://example.com/api/nodes/select for nodes.
	`)
	manifestUri  = replicate.Arg("manifest", "a path or url to a pod manifest that will be replicated.").Required().String()
	consulUrl    = replicate.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	consulToken  = replicate.Flag("token", "The ACL token to use for consul").String()
	headers      = replicate.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	nodeSelector = replicate.Flag("selector", "A selector specifying what nodes to deploy to").String()
	nodeEndpoint = replicate.Flag("node-endpoint", "An endpoint to query for node selector matches").String()
	nodes        = replicate.Flag("nodes", "The number of nodes to replicate to").Default("1").Int()
	https        = replicate.Flag("https", "Use HTTPS").Bool()
)

func consulClient(httpClient *http.Client) *api.Client {
	conf := api.DefaultConfig()
	conf.HttpClient = httpClient
	conf.Token = *consulToken
	if *consulUrl != "" {
		conf.Address = *consulUrl
	}
	if *https {
		conf.Scheme = "https"
	}
	// error is always nil
	consulClient, _ := api.NewClient(conf)
	return consulClient
}

func scheduler(httpClient *http.Client) rc.Scheduler {
	nodeUrl, err := url.Parse(*nodeEndpoint)
	if err != nil {
		log.Fatalf("Couldn't parse node endpoint: %s", err)
	}

	nodeApplicator, err := labels.NewHttpApplicator(httpClient, nodeUrl)
	if err != nil {
		log.Fatalf("Couldn't create node applicator: %s", err)
	}

	return rc.NewApplicatorScheduler(nodeApplicator)
}

func makeRcFields(stores ...rcstore.Store) []fields.RC {
	manifest, err := pods.ManifestFromURI(*manifestUri)
	if err != nil {
		log.Fatalf("%s", err)
	}

	nodeSelector, err := labels.Parse(*nodeSelector)
	if err != nil {
		log.Fatalf("Invalid node selector: %s", err)
	}

	sha, err := manifest.SHA()
	if err != nil {
		log.Fatalf("Can't get manifest sha: %s", err)
	}

	podLabels := map[string]string{
		"sha-truncated":     sha[:63],
		"mini-orchestrator": "true",
	}

	rcs := make([]fields.RC, len(stores))

	for i, store := range stores {
		err := store.SetDesiredReplicas(fields.ID("BOGUSBOGUS"), 1)
		if err == nil {
			log.Fatalf("Should have errored setting a nonexistent ID")
		}

		err = store.Disable(fields.ID("BOGUSBOGUS"))
		if err == nil {
			log.Fatalf("Should have errored disabling a nonexistent ID")
		}

		err = store.Delete(fields.ID("BOGUSBOGUS"))
		if err == nil {
			log.Fatalf("Should have errored deleting a nonexistent ID")
		}

		fields, err := store.Create(manifest, nodeSelector, podLabels)
		if err != nil {
			log.Fatalf("Couldn't create to-be-deleted replication controller in store %d: %s", i, err)
		}

		// Just kidding, immediately delete.
		err = store.Delete(fields.ID)
		if err != nil {
			log.Fatalf("Couldn't delete replication controller in store %d: %s", i, err)
		}

		fields, err = store.Create(manifest, nodeSelector, podLabels)
		if err != nil {
			log.Fatalf("Couldn't create to-be-returned replication controller in store %d: %s", i, err)
		}

		// If I get the manifest, It should give me the same results.
		check, err := store.Get(fields.ID)
		if err != nil {
			log.Fatalf("Couldn't get replication controller in store %d: %s", i, err)
		}

		mismatches := checkFieldsSame(fields, check)
		if mismatches == 0 {
			log.Printf("Match for store %d, congrats!", i)
		} else {
			// Errors already printed by checkFieldsSame
			log.Fatalf("Mismatch of controller in store %d with %d differences", i, mismatches)
		}

		listeds, err := store.List()
		if err != nil {
			log.Fatalf("Couldn't list in store %d: %s", i, err)
		}

		found := false
		for _, listed := range listeds {
			if listed.ID == fields.ID {
				found = true
				break
			}
		}
		if !found {
			log.Fatalf("List of store %s didn't list the just-created RC! +%v, looking for %s", i, listeds, fields.ID)
		}

		rcs[i] = fields
	}

	return rcs
}

func tryMatch(a, b, desc string) int {
	if a == b {
		return 0
	}
	log.Println("%s mismatch: %s != %s", desc, a, b)
	return 1
}

func checkFieldsSame(a, b fields.RC) int {
	mismatches := 0

	mismatches += tryMatch(a.ID.String(), b.ID.String(), "RC ID")
	mismatches += tryMatch(a.Manifest.ID(), b.Manifest.ID(), "Manifest ID")
	mismatches += tryMatch(a.NodeSelector.String(), b.NodeSelector.String(), "Node Selector")
	mismatches += tryMatch(a.PodLabels.String(), b.PodLabels.String(), "Node Selector")

	if a.Disabled != b.Disabled {
		mismatches += 1
		log.Println("Disabled mismatch: %s != %s", a.Disabled, b.Disabled)
	}

	if a.ReplicasDesired != b.ReplicasDesired {
		mismatches += 1
		log.Println("Replicas Desired mismatch: %s != %s", a.Disabled, b.Disabled)
	}

	return mismatches
}

func main() {
	replicate.Version(version.VERSION)
	replicate.Parse(os.Args[1:])

	httpClient := net.NewHeaderClient(*headers, http.DefaultTransport)
	consulClient := consulClient(httpClient)
	fakeStore := rcstore.NewFake()
	// Eh 3 retries, don't bother allow it to be configurable
	consulStore := rcstore.NewConsul(consulClient, 3, logging.DefaultLogger)
	fields := makeRcFields(fakeStore, consulStore)
	consulFields := fields[1]

	// TODO: Should the number of retries be configurable?
	podApplicator := labels.NewConsulApplicator(consulClient, 3)
	scheduler := scheduler(httpClient)

	replicationController := rc.New(
		consulFields,
		kp.NewFromClient(consulClient),
		consulStore,
		scheduler,
		podApplicator,
	)

	labeled, err := podApplicator.GetLabels(labels.RC, consulFields.ID.String())
	if err != nil {
		log.Fatalf("Couldn't get RC labels: %s", err)
	}
	log.Printf("RC labels y'all: %+v", labeled.Labels)

	quit := make(chan struct{})
	errors := replicationController.WatchDesires(quit)

	log.Printf("We're deployin', y'all! Setting desired replicas to %d", *nodes)
	err = consulStore.SetDesiredReplicas(consulFields.ID, *nodes)
	if err != nil {
		log.Fatalf("Couldn't set desired to %d: %s", *nodes, err)
	}

	currentNodes := []string{}

	for len(currentNodes) != *nodes {
		// TODO: Does this sleep loop imply we want a replicationController.WatchCurrentNodes() ???
		time.Sleep(3 * time.Second)
		var err error
		currentNodes, err = replicationController.CurrentNodes()
		if err != nil {
			log.Fatalf("Couldn't get current nodes: %s", err)
		}
		log.Printf("Currently on %v", currentNodes)
	}

	go func() {
		for err := range errors {
			log.Printf("Error from watcher: %s", err)
		}
	}()

	log.Println("Complete. Let's disable that RC.")
	err = consulStore.Disable(consulFields.ID)
	if err == nil {
		log.Println("Good!")
	} else {
		log.Fatalf("Couldn't disable: %s", err)
	}

	log.Println("Let's try to delete an RC with a desired count.")
	err = consulStore.Delete(consulFields.ID)
	if err == nil {
		log.Fatalf("Delete succeeded, should have errored!")
	} else {
		log.Printf("Good! Got an error. It was: %s", err)
	}

	log.Println("OK, what if we set the desired replicas to 0?")
	err = consulStore.SetDesiredReplicas(consulFields.ID, 0)
	if err == nil {
		log.Println("Good!")
	} else {
		log.Fatalf("Couldn't set desired to 0: %s", err)
	}

	log.Println("Deleting now should succeed.")
	err = consulStore.Delete(consulFields.ID)
	if err == nil {
		log.Println("Good!")
	} else {
		log.Fatalf("Couldn't delete: %s", err)
	}

	labeled, err = podApplicator.GetLabels(labels.RC, consulFields.ID.String())
	if err != nil {
		log.Fatalf("Couldn't get RC labels: %s", err)
	}
	log.Printf("RC labels after: %+v", labeled.Labels)
	if len(labeled.Labels) != 0 {
		log.Fatalf("Shouldn't have any labels anymore!")
	}

	log.Println("Asking watcher to quit now.")
	quit <- struct{}{}
}
