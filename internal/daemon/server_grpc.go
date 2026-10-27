package daemon

import (
	"context"
	"errors"
	"fmt"
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
	r, err := s.d.handleWhoami(id)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	resp := &driftnodepb.WhoamiResp{
		Identity:    r.Identity.String(),
		Token:       r.Token,
		Stable:      r.Stable,
		DisplayName: r.DisplayName,
		HasAvatar:   r.HasAvatar,
		Bio:         r.Bio,
		FirstName:   r.FirstName,
		LastName:    r.LastName,
		Location:    r.Location,
	}
	return resp, nil
}

func (s *grpcServer) Follows(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.IdentitiesResp, error) {
	follows, err := s.d.handleFollows()
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.IdentitiesResp{Identities: follows}, nil
}

func (s *grpcServer) Followers(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.IdentitiesResp, error) {
	followers, err := s.d.handleFollowers()
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.IdentitiesResp{Identities: followers}, nil
}

func (s *grpcServer) FeedPage(_ context.Context, req *driftnodepb.FeedPageReq) (*driftnodepb.FeedPageResp, error) {
	items, hasMore, err := s.d.handleFeedPage(int(req.Page), req.Mine, req.AllLikes)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.FeedPageResp{Items: feedItemsToProto(items), HasMore: hasMore}, nil
}

func (s *grpcServer) Like(ctx context.Context, req *driftnodepb.LikeReq) (*driftnodepb.LikeResp, error) {
	if req.PostId == "" {
		return nil, statusErr("post id required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	id, err := s.d.handleLike(req.PostId, kp)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.LikeResp{EventId: id.String()}, nil
}

func (s *grpcServer) FetchLikes(ctx context.Context, req *driftnodepb.FetchLikesReq) (*driftnodepb.FetchLikesResp, error) {
	if len(req.PostIds) == 0 {
		return nil, statusErr("at least one post id required")
	}
	ids := make([]core.EventID, 0, len(req.PostIds))
	for _, s := range req.PostIds {
		id, err := core.ParseEventID(s)
		if err != nil {
			return nil, statusErr(fmt.Sprintf("post id %q: %s", s, err))
		}
		ids = append(ids, id)
	}
	got, err := s.d.handleFetchLikes(ids)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	out := make([]string, 0, len(got))
	for _, se := range got {
		id, _ := se.ID()
		out = append(out, id.String())
	}
	return &driftnodepb.FetchLikesResp{LikeEventIds: out}, nil
}

func (s *grpcServer) Post(ctx context.Context, req *driftnodepb.PostReq) (*driftnodepb.PostResp, error) {
	if req.Text == "" {
		return nil, statusErr("text required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	id, err := s.d.handlePost(req.Text, req.ParentId, kp)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.PostResp{EventId: id.String()}, nil
}

func (s *grpcServer) Profile(ctx context.Context, req *driftnodepb.ProfileReq) (*driftnodepb.EmptyResp, error) {
	if req.Name == "" {
		return nil, statusErr("name required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	if _, err := s.d.handleProfile(req.Name, kp); err != nil {
		return nil, statusErr(err.Error())
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
	if _, err := s.d.handleDetail(req.Bio, req.FirstName, req.LastName, req.Location, kp); err != nil {
		return nil, statusErr(err.Error())
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

func (s *grpcServer) Pin(ctx context.Context, req *driftnodepb.IdentityReq) (*driftnodepb.VerifyResp, error) {
	if req.Identity == "" {
		return nil, statusErr("identity required")
	}
	id, err := core.ParseIdentity(req.Identity)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	if err := s.d.store.PinIdentity(id); err != nil {
		return nil, statusErr(err.Error())
	}
	s.d.logger.Info("follow pinned", "identity", id)
	s.d.refreshSplit()
	return &driftnodepb.VerifyResp{Identity: string(id)}, nil
}

func (s *grpcServer) Unpin(ctx context.Context, req *driftnodepb.IdentityReq) (*driftnodepb.VerifyResp, error) {
	if req.Identity == "" {
		return nil, statusErr("identity required")
	}
	id, err := core.ParseIdentity(req.Identity)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	if err := s.d.store.UnpinIdentity(id); err != nil {
		return nil, statusErr(err.Error())
	}
	s.d.refreshSplit()
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
	followed, err := s.d.handleFollow(req.Target, kp)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.FollowResp{Followed: followed.String()}, nil
}

func (s *grpcServer) Unfollow(ctx context.Context, req *driftnodepb.UnfollowReq) (*driftnodepb.UnfollowResp, error) {
	if req.Target == "" {
		return nil, statusErr("target required")
	}
	kp, err := s.d.signingKey(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	unfollowed, err := s.d.handleUnfollow(req.Target, kp)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.UnfollowResp{Unfollowed: unfollowed.String()}, nil
}

func (s *grpcServer) Unlock(ctx context.Context, req *driftnodepb.UnlockReq) (*driftnodepb.UnlockResp, error) {
	if req.Passphrase == "" {
		return nil, statusErr("passphrase required")
	}
	kp, err := s.d.handleUnlock(req.Passphrase)
	if err != nil {
		return nil, statusErr(err.Error())
	}
	return &driftnodepb.UnlockResp{Status: "unlocked", Identity: kp.Identity().String()}, nil
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
	s.d.triggerCrawlNow()
	s.d.logger.Info("sync requested")
	return &driftnodepb.EmptyResp{Status: "triggered"}, nil
}

func (s *grpcServer) Prune(ctx context.Context, _ *driftnodepb.Empty) (*driftnodepb.PruneResp, error) {
	n, err := s.d.store.PruneRemotePosts()
	if err != nil {
		return nil, statusErr(err.Error())
	}
	s.d.logger.Info("prune requested", "pruned", n)
	return &driftnodepb.PruneResp{Pruned: int32(n)}, nil
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
		Unlocked:  unlocked,
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
	snap := s.d.snapshot()
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
