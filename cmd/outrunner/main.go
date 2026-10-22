package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	outrunner "github.com/NetwindHQ/gha-outrunner"
	"github.com/NetwindHQ/gha-outrunner/provisioner/docker"
	"github.com/NetwindHQ/gha-outrunner/provisioner/libvirt"
	"github.com/NetwindHQ/gha-outrunner/provisioner/tart"
	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

// version is set at build time by GoReleaser via ldflags.
var version = "dev"

var cfg struct {
	URL        string
	Token      string
	MaxRunners int
	ConfigFile string
}

var rootCmd = &cobra.Command{
	Use:     "outrunner",
	Short:   "Ephemeral GitHub Actions runners, no Kubernetes required",
	Version: version,
	Long: `outrunner provisions ephemeral Docker containers and/or VMs for each
GitHub Actions job. It uses the scaleset API to register as an autoscaling
runner group, then creates and destroys runner environments on demand.

Each runner definition in the config file gets its own scale set. GitHub
routes jobs to the correct scale set based on labels. Local runner groups
share process-local capacity without changing GitHub runner groups.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		return run(ctx)
	},
}

func init() {
	f := rootCmd.Flags()
	f.StringVar(&cfg.URL, "url", "", "Repository or org URL (overrides config file)")
	f.StringVar(&cfg.Token, "token", "", "GitHub PAT (overrides GITHUB_TOKEN env var and config)")
	f.IntVar(&cfg.MaxRunners, "max-runners", 2, "Default max concurrent runners per scale set")
	f.StringVar(&cfg.ConfigFile, "config", "/etc/outrunner/config.yml", "Config file path (YAML)")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	logger := slog.New(outrunner.NewSimpleHandler(os.Stdout, slog.LevelInfo))
	listenerLogger := slog.New(outrunner.NewSimpleHandler(os.Stdout, slog.LevelWarn))
	log.SetOutput(io.Discard)

	config, err := outrunner.LoadConfig(cfg.ConfigFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger.Info("Loaded config", slog.Int("runners", len(config.Runners)))

	// Resolve (url, token) per runner and deduplicate clients.
	type clientKey struct{ url, token string }
	clients := make(map[clientKey]*scaleset.Client)

	type resolvedRunner struct {
		name       string
		runner     outrunner.RunnerConfig
		maxRunners int
		key        clientKey
		group      *outrunner.GroupScheduler
	}
	groups := make(map[string]*outrunner.GroupScheduler)
	var resolved []resolvedRunner

	names := make([]string, 0, len(config.Runners))
	for name := range config.Runners {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		runner := config.Runners[name]
		url, err := outrunner.ResolveRunnerURL(cfg.URL, config, &runner)
		if err != nil {
			return fmt.Errorf("runner %s: %w", name, err)
		}
		token, err := outrunner.ResolveRunnerToken(cfg.Token, config, &runner)
		if err != nil {
			return fmt.Errorf("runner %s: %w", name, err)
		}

		maxRunners := runner.MaxRunners
		if maxRunners == 0 {
			maxRunners = cfg.MaxRunners
		}
		var group *outrunner.GroupScheduler
		if runner.Group != "" {
			group = groups[runner.Group]
			if group == nil {
				group, err = outrunner.NewGroupScheduler(config.Groups[runner.Group].MaxRunners)
				if err != nil {
					return fmt.Errorf("group %s: %w", runner.Group, err)
				}
				groups[runner.Group] = group
			}
			if err := group.RegisterPool(name, maxRunners); err != nil {
				return fmt.Errorf("runner %s: register group pool: %w", name, err)
			}
		}

		key := clientKey{url: url, token: token}
		if _, ok := clients[key]; !ok {
			c, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
				GitHubConfigURL:     url,
				PersonalAccessToken: token,
			})
			if err != nil {
				return fmt.Errorf("runner %s: create scaleset client: %w", name, err)
			}
			clients[key] = c
			logger.Info("Created scaleset client", slog.String("url", url))
		}
		resolved = append(resolved, resolvedRunner{name: name, runner: runner, maxRunners: maxRunners, key: key, group: group})
	}

	// Hold exclusive host ownership and reconcile all managed Tart resources
	// before any listener can advertise or provision capacity.
	var hostLock *os.File
	var tartBases, tartPrefixes []string
	for _, item := range resolved {
		if item.runner.Tart != nil {
			tartBases = append(tartBases, item.runner.Tart.Image)
			tartPrefixes = append(tartPrefixes, item.name+"-")
		}
	}
	if len(tartPrefixes) > 0 {
		hostLock, err = tart.AcquireHost()
		if err != nil {
			return fmt.Errorf("acquire Tart host ownership: %w", err)
		}
		defer func() {
			if closeErr := hostLock.Close(); closeErr != nil {
				logger.Error("Failed to release Tart host ownership", slog.String("error", closeErr.Error()))
			}
		}()
		for _, item := range resolved {
			if item.runner.Tart == nil {
				continue
			}
			prov := tart.New(logger.WithGroup("tart"), tartBases, tartPrefixes)
			if err := prov.Reconcile(ctx, tartPrefixes, tartBases); err != nil {
				if closeErr := prov.Close(); closeErr != nil {
					logger.Error("Failed to close Tart reconciler", slog.String("error", closeErr.Error()))
				}
				return fmt.Errorf("reconcile Tart host: %w", err)
			}
			if err := prov.Close(); err != nil {
				return fmt.Errorf("close Tart reconciler: %w", err)
			}
			break
		}
	}
	for _, item := range resolved {
		if item.runner.Tart != nil {
			continue
		}
		prov, err := createProvisioner(logger.With(slog.String("scaleSet", item.name)), &item.runner, tartBases, tartPrefixes)
		if err != nil {
			return fmt.Errorf("runner %s: create cleanup provisioner: %w", item.name, err)
		}
		if cleaner, ok := prov.(interface{ Cleanup(string) }); ok {
			cleaner.Cleanup(item.name + "-")
		}
		if err := prov.Close(); err != nil {
			return fmt.Errorf("runner %s: close cleanup provisioner: %w", item.name, err)
		}
	}

	workerGroup, workerCtx := errgroup.WithContext(ctx)
	for _, item := range resolved {
		item := item
		client := clients[item.key]
		workerGroup.Go(func() error {
			return runWorker(workerCtx, logger, listenerLogger, client, item.name, &item.runner, item.maxRunners, item.group, tartBases, tartPrefixes)
		})
	}
	if err := workerGroup.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("Shut down cleanly")
	return nil
}

func runWorker(ctx context.Context, logger, listenerLogger *slog.Logger, client *scaleset.Client, name string, runner *outrunner.RunnerConfig, maxRunners int, group *outrunner.GroupScheduler, tartBases, tartPrefixes []string) (retErr error) {
	logger = logger.With(slog.String("scaleSet", name))
	prov, err := createProvisioner(logger, runner, tartBases, tartPrefixes)
	if err != nil {
		return fmt.Errorf("runner %s: %w", name, err)
	}
	defer func() {
		if closeErr := prov.Close(); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("runner %s: close provisioner: %w", name, closeErr))
		}
	}()

	var labels []scaleset.Label
	for _, l := range runner.Labels {
		labels = append(labels, scaleset.Label{Name: l, Type: "User"})
	}

	// Get or create scale set
	logger.Info("Looking for scale set")
	scaleSet, err := client.GetRunnerScaleSet(ctx, 1, name)
	if err != nil {
		return fmt.Errorf("runner %s: get scale set: %w", name, err)
	}
	if scaleSet == nil {
		logger.Info("Creating scale set")
		scaleSet, err = client.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
			Name:          name,
			RunnerGroupID: 1,
			Labels:        labels,
			RunnerSetting: scaleset.RunnerSetting{
				DisableUpdate: true,
			},
		})
		if err != nil {
			return fmt.Errorf("runner %s: create scale set: %w", name, err)
		}
	} else if !labelsMatch(scaleSet.Labels, labels) {
		logger.Info("Updating scale set labels")
		scaleSet.Labels = labels
		scaleSet, err = client.UpdateRunnerScaleSet(ctx, scaleSet.ID, scaleSet)
		if err != nil {
			return fmt.Errorf("runner %s: update scale set labels: %w", name, err)
		}
	}
	logger.Info("Scale set ready", slog.Int("id", scaleSet.ID))

	// Scale sets are reused across restarts. No deletion on shutdown.

	// Create message session
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = uuid.NewString()
	}

	sessionClient, err := client.MessageSessionClient(ctx, scaleSet.ID, hostname)
	if err != nil {
		return fmt.Errorf("runner %s: create message session: %w", name, err)
	}
	defer func() { _ = sessionClient.Close(context.Background()) }()

	var messageClient listener.Client = sessionClient
	listenerMax := maxRunners
	if group != nil {
		messageClient = outrunner.NewGroupMessageClient(sessionClient, group, name)
		listenerMax = 0
	}
	l, err := listener.New(messageClient, listener.Config{
		ScaleSetID: scaleSet.ID,
		MaxRunners: listenerMax,
		Logger:     listenerLogger.WithGroup("listener"),
	})
	if err != nil {
		return fmt.Errorf("runner %s: create listener: %w", name, err)
	}
	scaler := outrunner.NewScaler(
		logger.WithGroup("scaler"),
		client, scaleSet.ID, maxRunners, name, runner, prov, group,
	)

	logger.Info("Listening for jobs", slog.Int("maxRunners", maxRunners))
	err = l.Run(ctx, scaler)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	scaler.Shutdown(shutdownCtx)

	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("runner %s: listener: %w", name, err)
	}
	return nil
}

func createProvisioner(logger *slog.Logger, runner *outrunner.RunnerConfig, tartBases, tartPrefixes []string) (outrunner.Provisioner, error) {
	switch runner.ProviderType() {
	case "docker":
		return docker.New(logger.WithGroup("docker"))
	case "libvirt":
		return libvirt.New(logger.WithGroup("libvirt"), libvirt.Config{
			Socket: runner.Libvirt.Socket,
		})
	case "tart":
		return tart.New(logger.WithGroup("tart"), tartBases, tartPrefixes), nil
	default:
		return nil, fmt.Errorf("unknown provider type %q", runner.ProviderType())
	}
}

// labelsMatch checks if the existing scale set labels match the desired labels.
func labelsMatch(existing []scaleset.Label, desired []scaleset.Label) bool {
	if len(existing) != len(desired) {
		return false
	}
	have := make(map[string]bool, len(existing))
	for _, l := range existing {
		have[l.Name] = true
	}
	for _, l := range desired {
		if !have[l.Name] {
			return false
		}
	}
	return true
}
