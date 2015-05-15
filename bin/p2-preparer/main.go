package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/version"
)

func main() {
	logger := logging.NewLogger(logrus.Fields{})
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		logger.NoFields().Fatalln("No CONFIG_PATH variable was given")
	}
	preparerConfig, err := preparer.LoadPreparerConfig(configPath)
	if err != nil {
		logger.WithField("inner_err", err).Fatalln("could not load preparer config")
	}

	prep, err := preparer.New(preparerConfig, logger)
	if err != nil {
		logger.WithField("inner_err", err).Fatalln("Could not initialize preparer")
	}
	defer prep.Close()

	logger.WithFields(logrus.Fields{
		"starting":  true,
		"node_name": preparerConfig.NodeName,
		"consul":    preparerConfig.ConsulAddress,
		"hooks_dir": preparerConfig.HooksDirectory,
		"keyring":   preparerConfig.Auth["keyring"],
		"version":   version.VERSION,
	}).Infoln("Preparer started successfully")

	successMainUpdate := make(chan struct{})
	successHookUpdate := make(chan struct{})
	errMainUpdate := make(chan error)
	errHookUpdate := make(chan error)
	quitMainUpdate := make(chan struct{})
	quitHookUpdate := make(chan struct{})
	go prep.WatchForPodManifestsForNode(successMainUpdate, errMainUpdate, quitMainUpdate)
	go prep.WatchForHooks(successHookUpdate, errHookUpdate, quitHookUpdate)

	http.HandleFunc("/_status", statusHandler(successMainUpdate, successHookUpdate, errMainUpdate, errHookUpdate))
	go http.ListenAndServe(":8080", nil)

	waitForTermination(logger, quitMainUpdate, quitHookUpdate)

	logger.NoFields().Infoln("Terminating")
}

func watchForErrors(successes <-chan struct{}, errs <-chan error, consecutive *int, lastError *error) {
	for {
		select {
		case err := <-errs:
			*lastError = err
			*consecutive++
		case <-successes:
			*consecutive = 0
		}
	}
}

func statusHandler(mainSuccesses, hookSuccesses <-chan struct{}, mainErrors, hookErrors <-chan error) http.HandlerFunc {
	consecutiveMainErrors := 0
	consecutiveHookErrors := 0
	var lastMainError error
	var lastHookError error

	go watchForErrors(mainSuccesses, mainErrors, &consecutiveMainErrors, &lastMainError)
	go watchForErrors(hookSuccesses, hookErrors, &consecutiveHookErrors, &lastHookError)

	return func(w http.ResponseWriter, r *http.Request) {
		status := "OK"

		type StatusResponse struct {
			Status                string `json:"status"`
			ConsecutiveHookErrors int    `json:"consecutive_hook_errors"`
			ConsecutivePodErrors  int    `json:"consecutive_pod_errors"`
			LastHookError         string `json:"last_hook_error"`
			LastPodError          string `json:"last_pod_error"`
		}

		response, err := json.Marshal(StatusResponse{
			Status:                status,
			ConsecutiveHookErrors: consecutiveHookErrors,
			ConsecutivePodErrors:  consecutiveMainErrors,
			LastHookError:         fmt.Sprintf("%+v", lastHookError),
			LastPodError:          fmt.Sprintf("%+v", lastMainError),
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "{status:\"Unknown (couldn't marshal: %s)\"}\n", err)
		} else {
			fmt.Fprintf(w, "%s\n", response)
		}
	}
}

func waitForTermination(logger logging.Logger, quitMainUpdate, quitHookUpdate chan struct{}) {
	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)
	received := <-signalCh
	logger.WithField("signal", received.String()).Infoln("Stopping work")
	quitHookUpdate <- struct{}{}
	quitMainUpdate <- struct{}{}
	<-quitMainUpdate // acknowledgement
}
