package tenantbackup

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// leaseBinding preserves grouping while using fresh destination lease IDs.
// TTL is the remaining lifetime captured in the archive, not the original
// granted lifetime. The etcd server may round a grant up to its minimum TTL.
type leaseBinding struct {
	TTL         int64            `json:"ttl"`
	Destination clientv3.LeaseID `json:"destination"`
}

func archiveLeasePlan(spool *os.File) (map[string]leaseBinding, error) {
	if _, err := spool.Seek(0, 0); err != nil {
		return nil, err
	}
	plan := map[string]leaseBinding{}
	scan := bufio.NewScanner(spool)
	scan.Buffer(make([]byte, 64*1024), maxLineBytes)
	for scan.Scan() {
		var rec record
		if err := json.Unmarshal(scan.Bytes(), &rec); err != nil {
			return nil, errors.New("invalid private restore spool")
		}
		if rec.Lease != 0 {
			plan[strconv.FormatInt(rec.Lease, 10)] = leaseBinding{TTL: rec.TTL}
		}
	}
	return plan, scan.Err()
}

func validateLeaseBindings(plan, bindings map[string]leaseBinding, complete bool) error {
	if complete && len(plan) != len(bindings) {
		return errors.New("restore checkpoint has an incomplete lease mapping")
	}
	seen := map[clientv3.LeaseID]bool{}
	for source, binding := range bindings {
		expected, exists := plan[source]
		if !exists || binding.TTL != expected.TTL || binding.Destination == 0 || seen[binding.Destination] {
			return errors.New("restore checkpoint lease mapping does not match archive")
		}
		seen[binding.Destination] = true
	}
	return nil
}

type leaseSession struct {
	ctx    context.Context
	cancel context.CancelFunc
	api    clientv3.Lease
	ids    []clientv3.LeaseID
	failed chan error
	wg     sync.WaitGroup
}

func (s *leaseSession) close() { s.cancel(); s.wg.Wait() }
func (s *leaseSession) check() error {
	select {
	case err := <-s.failed:
		return err
	default:
		return s.ctx.Err()
	}
}

func (s *leaseSession) watch(id clientv3.LeaseID) error {
	responses, err := s.api.KeepAlive(s.ctx, id)
	if err != nil {
		return fmt.Errorf("start restore lease renewal: %w", err)
	}
	select {
	case first, ok := <-responses:
		if !ok || first == nil || first.ID != id || first.TTL <= 0 {
			return errors.New("restore lease renewal did not start")
		}
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	s.ids = append(s.ids, id)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for response := range responses {
			if response == nil || response.ID != id || response.TTL <= 0 {
				select {
				case s.failed <- errors.New("restore lease renewal failed; resume while tenant remains stopped"):
				default:
				}
				return
			}
		}
		if s.ctx.Err() == nil {
			select {
			case s.failed <- errors.New("restore lease renewal stopped; resume while tenant remains stopped"):
			default:
			}
		}
	}()
	return nil
}

func (s *leaseSession) refresh() error {
	if err := s.check(); err != nil {
		return err
	}
	for _, id := range s.ids {
		response, err := s.api.KeepAliveOnce(s.ctx, id)
		if err != nil {
			return fmt.Errorf("refresh restore lease: %w", err)
		}
		if response == nil || response.ID != id || response.TTL <= 0 {
			return errors.New("restore lease expired before completion")
		}
	}
	return s.check()
}

// prepareRestoreLeases reuses only this restore's checkpointed fresh leases.
// If an interrupted attempt's lease expired, copy is replayed from the start
// using new leases without clearing data a second time. Empty grants from an
// unknown/failed operation expire naturally; source leases are never revoked.
func prepareRestoreLeases(ctx context.Context, api clientv3.Lease, plan, current map[string]leaseBinding) (bindings map[string]leaseBinding, session *leaseSession, changed bool, err error) {
	leaseCtx, cancel := context.WithCancel(ctx)
	session = &leaseSession{ctx: leaseCtx, cancel: cancel, api: api, failed: make(chan error, 1)}
	defer func() {
		if err != nil {
			session.close()
		}
	}()
	bindings = map[string]leaseBinding{}
	sources := make([]string, 0, len(plan))
	for source := range plan {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	for _, source := range sources {
		binding, exists := current[source]
		live := false
		if exists {
			var response *clientv3.LeaseTimeToLiveResponse
			response, err = api.TimeToLive(leaseCtx, binding.Destination, clientv3.WithAttachedKeys())
			if err != nil {
				return nil, session, false, fmt.Errorf("read restore lease: %w", err)
			}
			live = response != nil && response.ID == binding.Destination && response.TTL > 0
		}
		if !live {
			var response *clientv3.LeaseGrantResponse
			response, err = api.Grant(leaseCtx, plan[source].TTL)
			if err != nil {
				return nil, session, false, fmt.Errorf("grant fresh restore lease: %w", err)
			}
			if response == nil || response.Error != "" || response.ID == 0 || response.TTL <= 0 || response.TTL > MaxLeaseTTL {
				return nil, session, false, errors.New("invalid restore lease grant")
			}
			binding = leaseBinding{TTL: plan[source].TTL, Destination: response.ID}
			changed = true
		}
		bindings[source] = binding
		if err = session.watch(binding.Destination); err != nil {
			return nil, session, false, err
		}
	}
	return bindings, session, changed, nil
}
