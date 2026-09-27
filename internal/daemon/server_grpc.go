package daemon

import (
	"context"
	"errors"
	"io"
	"net"

	"driftnode/internal/core"
	"driftnode/internal/proto/driftnodepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// grpcServer adapts the existing daemon to the generated DriftnodeServer
// interface. The dispatch handlers keep returning the legacy Response value
// (woven through tests and the sync loop), so the server converts at the
// boundary: a non-empty Response.Error becomes a gRPC error, otherwise the
// untyped Result is mapped to the typed proto response.
type grpcServer struct {
	driftnodepb.UnimplementedDriftnodeServer
	d *Daemon
}

// statusErr wraps a daemon error string into a gRPC status error so the
// client can extract the message via status.Convert.
func statusErr(msg string) error {
	if msg == "" {
		return nil
	}
	return status.Error(codes.FailedPrecondition, msg)
}

// RegisterServer attaches the daemon's gRPC service to srv.
func RegisterServer(srv *grpc.Server, d *Daemon) {
	driftnodepb.RegisterDriftnodeServer(srv, &grpcServer{d: d})
}

func (s *grpcServer) Whoami(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.WhoamiResp, error) {
	id, ok, err := s.d.store.Identity()
	if err != nil {
		return nil, statusErr(err.Error())
	}
	if !ok {
		return nil, statusErr("no identity")
	}
	r := s.d.handleWhoami(id)
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	m, _ := r.Result.(map[string]any)
	resp := &driftnodepb.WhoamiResp{Identity: id.String()}
	if m != nil {
		if v, ok := m["token"].(string); ok {
			resp.Token = v
		}
		if v, ok := m["stable"].(bool); ok {
			resp.Stable = v
		}
		if v, ok := m["display_name"].(string); ok {
			resp.DisplayName = v
		}
		if v, ok := m["has_avatar"].(bool); ok {
			resp.HasAvatar = v
		}
		if v, ok := m["bio"].(string); ok {
			resp.Bio = v
		}
		if v, ok := m["first_name"].(string); ok {
			resp.FirstName = v
		}
		if v, ok := m["last_name"].(string); ok {
			resp.LastName = v
		}
		if v, ok := m["location"].(string); ok {
			resp.Location = v
		}
	}
	return resp, nil
}

func (s *grpcServer) Follows(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.IdentitiesResp, error) {
	r := s.d.handleFollows()
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	m, ok := r.Result.(map[string]any)
	if !ok {
		return &driftnodepb.IdentitiesResp{}, nil
	}
	raw, _ := m["follows"].([]map[string]string)
	return &driftnodepb.IdentitiesResp{Identities: toIdentities(raw)}, nil
}

func (s *grpcServer) Followers(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.IdentitiesResp, error) {
	r := s.d.handleFollowers()
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	m, ok := r.Result.(map[string]any)
	if !ok {
		return &driftnodepb.IdentitiesResp{}, nil
	}
	raw, _ := m["followers"].([]map[string]string)
	return &driftnodepb.IdentitiesResp{Identities: toIdentities(raw)}, nil
}

func (s *grpcServer) Feed(ctx context.Context, req *driftnodepb.FeedReq) (*driftnodepb.FeedResp, error) {
	r := s.d.handleFeed(int(req.Limit))
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	items, _ := r.Result.([]feedItem)
	return &driftnodepb.FeedResp{Items: toFeedItems(items)}, nil
}

func (s *grpcServer) Post(ctx context.Context, req *driftnodepb.PostReq) (*driftnodepb.PostResp, error) {
	if req.Text == "" {
		return nil, statusErr("text required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	r := s.d.handlePost(req.Text, kp)
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	m, _ := r.Result.(map[string]string)
	resp := &driftnodepb.PostResp{}
	if m != nil {
		resp.EventId = m["event_id"]
	}
	return resp, nil
}

func (s *grpcServer) Profile(ctx context.Context, req *driftnodepb.ProfileReq) (*driftnodepb.EmptyResp, error) {
	if req.Name == "" {
		return nil, statusErr("name required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	r := s.d.handleProfile(req.Name, kp)
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	return &driftnodepb.EmptyResp{Status: "ok"}, nil
}

func (s *grpcServer) Detail(ctx context.Context, req *driftnodepb.DetailReq) (*driftnodepb.EmptyResp, error) {
	if req.Bio == "" && req.FirstName == "" && req.LastName == "" && req.Location == "" {
		return nil, statusErr("at least one detail field required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	r := s.d.handleDetail(req.Bio, req.FirstName, req.LastName, req.Location, kp)
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	return &driftnodepb.EmptyResp{Status: "ok"}, nil
}

func (s *grpcServer) Zens(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.ZensResp, error) {
	// Copy each zenInfo by value under the lock so a concurrent
	// setZenStatus cannot race the enrichment reads below.
	s.d.mu.Lock()
	zens := make([]zenInfo, 0, len(s.d.zens))
	for _, z := range s.d.zens {
		zens = append(zens, *z)
	}
	s.d.mu.Unlock()
	out := make([]*driftnodepb.Zen, 0, len(zens))
	for i := range zens {
		out = append(out, s.d.zenToProto(zens[i]))
	}
	return &driftnodepb.ZensResp{Zens: out}, nil
}

func (s *grpcServer) Verify(ctx context.Context, req *driftnodepb.IdentityReq) (*driftnodepb.VerifyResp, error) {
	if req.Identity == "" {
		return nil, statusErr("identity required")
	}
	id, err := core.ParseIdentity(req.Identity)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	if err := s.d.store.VerifyIdentity(id); err != nil {
		return nil, statusErr(err.Error())
	}
	s.d.logger.Info("identity verified out-of-band", "identity", id)
	return &driftnodepb.VerifyResp{Identity: string(id)}, nil
}

func (s *grpcServer) Unverify(ctx context.Context, req *driftnodepb.IdentityReq) (*driftnodepb.VerifyResp, error) {
	if req.Identity == "" {
		return nil, statusErr("identity required")
	}
	id, err := core.ParseIdentity(req.Identity)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	if err := s.d.store.UnverifyIdentity(id); err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.VerifyResp{Identity: string(id)}, nil
}

func (s *grpcServer) Follow(ctx context.Context, req *driftnodepb.FollowReq) (*driftnodepb.FollowResp, error) {
	if req.Target == "" {
		return nil, statusErr("target required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	r := s.d.handleFollow(req.Target, kp)
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	m, _ := r.Result.(map[string]string)
	resp := &driftnodepb.FollowResp{}
	if m != nil {
		resp.Followed = m["followed"]
	}
	return resp, nil
}

func (s *grpcServer) Unfollow(ctx context.Context, req *driftnodepb.UnfollowReq) (*driftnodepb.UnfollowResp, error) {
	if req.Target == "" {
		return nil, statusErr("target required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	r := s.d.handleUnfollow(req.Target, kp)
	if r.Error != "" {
		return nil, statusErr(r.Error)
	}
	m, _ := r.Result.(map[string]string)
	resp := &driftnodepb.UnfollowResp{}
	if m != nil {
		resp.Unfollowed = m["unfollowed"]
	}
	return resp, nil
}

func (s *grpcServer) Unlock(ctx context.Context, req *driftnodepb.UnlockReq) (*driftnodepb.UnlockResp, error) {
	if req.Passphrase == "" {
		return nil, statusErr("passphrase required")
	}
	ur := s.d.handleUnlock(req.Passphrase)
	if ur.resp.Error != "" {
		return nil, statusErr(ur.resp.Error)
	}
	m, _ := ur.resp.Result.(map[string]string)
	resp := &driftnodepb.UnlockResp{Status: "unlocked"}
	if m != nil {
		resp.Identity = m["identity"]
	}
	return resp, nil
}

func (s *grpcServer) Lock(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.EmptyResp, error) {
	s.d.mu.Lock()
	s.d.unlocked = nil
	s.d.mu.Unlock()
	s.d.logger.Info("key locked")
	s.d.emitStatus()
	return &driftnodepb.EmptyResp{Status: "locked"}, nil
}

func (s *grpcServer) Sync(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.EmptyResp, error) {
	s.d.triggerSyncNow()
	s.d.logger.Info("sync requested")
	return &driftnodepb.EmptyResp{Status: "triggered"}, nil
}

func (s *grpcServer) Stop(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.EmptyResp, error) {
	// Shut down asynchronously so this handler returns and the ack is
	// written before Stop's GracefulStop waits for in-flight RPCs. Running
	// Stop synchronously here would deadlock: GracefulStop waits for the
	// Stop RPC to finish, which is the one calling it.
	go s.d.Stop()
	return &driftnodepb.EmptyResp{Status: "stopping"}, nil
}

func (s *grpcServer) Status(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.StatusResp, error) {
	s.d.mu.Lock()
	tcUp := s.d.tcListener != nil
	unlocked := s.d.unlocked != nil
	zenCount := int32(len(s.d.zens))
	socket := s.d.socket
	s.d.mu.Unlock()
	return &driftnodepb.StatusResp{
		Running:   true,
		Zens:      zenCount,
		Socket:    socket,
		Transport: tcUp,
		Unlocked:   unlocked,
	}, nil
}

func (s *grpcServer) RotateKey(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.RotateKeyResp, error) {
	newAddr, err := s.d.rotateKey()
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.RotateKeyResp{Token: newAddr}, nil
}

func (s *grpcServer) Subscribe(_ *driftnodepb.SubscribeReq, srv driftnodepb.Driftnode_SubscribeServer) error {
	// Register the subscriber before building the snapshot. A state change
	// after this point is then either reflected in the snapshot (the mutator
	// invalidated the feed cache before snapshot built it) or delivered as a
	// diff on the queue. Registering after the snapshot would drop events in
	// the gap between snapshot build and registration.
	sub := s.d.addSubscriberProto()
	defer s.d.removeSubscriber(sub)
	snap := s.d.snapshotProto()
	if err := srv.Send(&driftnodepb.Event{Kind: &driftnodepb.Event_Snapshot{Snapshot: snap}}); err != nil {
		return err
	}
	for {
		select {
		case ev, ok := <-sub.events:
			if !ok {
				return nil
			}
			if err := srv.Send(ev); err != nil {
				return err
			}
		case <-sub.done:
			return nil
		case <-srv.Context().Done():
			return srv.Context().Err()
		}
	}
}

// toFeedItems converts the daemon's internal feedItem slice to proto.
func toFeedItems(items []feedItem) []*driftnodepb.FeedItem {
	out := make([]*driftnodepb.FeedItem, 0, len(items))
	for _, it := range items {
		out = append(out, &driftnodepb.FeedItem{
			Id:        it.ID,
			Timestamp: it.Timestamp,
			Author:    it.Author,
			Name:      it.Name,
			Text:      it.Text,
		})
	}
	return out
}

// toIdentities converts the daemon's map[string]string identity rows to proto.
func toIdentities(rows []map[string]string) []*driftnodepb.Identity {
	out := make([]*driftnodepb.Identity, 0, len(rows))
	for _, r := range rows {
		out = append(out, &driftnodepb.Identity{
			Identity: r["identity"],
			Name:     r["name"],
		})
	}
	return out
}

// ---- client-side dialer (shared by CLI and TUI) ----

// DialClient connects to the daemon's gRPC control socket and returns a typed
// client. It returns ErrNotRunning if no daemon is listening.
func DialClient(socketPath string) (driftnodepb.DriftnodeClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, ErrNotRunning
	}
	// NewClient is lazy: it does not connect until the first RPC. Probe the
	// listener so a missing daemon surfaces as ErrNotRunning immediately,
	// matching the old Dial behavior callers depend on.
	c, err := net.Dial("unix", socketPath)
	if err != nil {
		conn.Close()
		return nil, nil, ErrNotRunning
	}
	c.Close()
	return driftnodepb.NewDriftnodeClient(conn), conn, nil
}

// errToStatus extracts the gRPC status message from an error, returning the
// original error string if it is not a gRPC status error.
func errToStatus(err error) string {
	if err == nil {
		return ""
	}
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

// IsStreamClosed reports whether err indicates the subscribe stream ended
// (client disconnect, daemon drop, or clean shutdown), so the TUI can
// reconnect instead of treating it as a fatal error.
func IsStreamClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.Canceled || s.Code() == codes.Unavailable {
			return true
		}
	}
	return false
}
