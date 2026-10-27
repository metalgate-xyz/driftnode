package daemon

// Legacy test-support client: drives the daemon's typed gRPC surface by
// method name, returning results in the Response shape the RPC tests were
// written against, so they read as table-driven scenarios.

import (
	"context"
	"fmt"

	"driftnode/internal/proto/driftnodepb"
)

// Response carries the result or error string of one SendRequest call.
type Response struct {
	Result any
	Error  string
}

// SendRequest dials the daemon and maps a method name to a typed RPC,
// returning the result in the legacy Response shape with map[string]any
// params, for tests. Production callers use DialClient and the typed client
// directly.
func SendRequest(socketPath, method string, params map[string]any) (*Response, error) {
	cl, cc, err := DialClient(socketPath)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	ctx := context.Background()
	switch method {
	case "whoami":
		r, err := cl.Whoami(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"identity": r.Identity, "token": r.Token, "stable": r.Stable, "display_name": r.DisplayName, "has_avatar": r.HasAvatar, "bio": r.Bio, "first_name": r.FirstName, "last_name": r.LastName, "location": r.Location}}, nil
	case "follows":
		r, err := cl.Follows(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"follows": identitiesFromProto(r.Identities)}}, nil
	case "followers":
		r, err := cl.Followers(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"followers": identitiesFromProto(r.Identities)}}, nil
	case "feed":
		mine := false
		if v, ok := params["mine"]; ok {
			if b, ok := v.(bool); ok {
				mine = b
			}
		}
		r, err := cl.FeedPage(ctx, &driftnodepb.FeedPageReq{Page: 0, Mine: mine})
		if err != nil {
			return nil, err
		}
		return &Response{Result: feedItemsFromProto(r.Items)}, nil
	case "post":
		r, err := cl.Post(ctx, &driftnodepb.PostReq{Text: paramString(params, "text"), Passphrase: paramString(params, "passphrase")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"event_id": r.EventId}}, nil
	case "profile":
		if _, err := cl.Profile(ctx, &driftnodepb.ProfileReq{Name: paramString(params, "name"), Passphrase: paramString(params, "passphrase")}); err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": "ok"}}, nil
	case "detail":
		if _, err := cl.Detail(ctx, &driftnodepb.DetailReq{Bio: paramString(params, "bio"), FirstName: paramString(params, "first_name"), LastName: paramString(params, "last_name"), Location: paramString(params, "location"), Passphrase: paramString(params, "passphrase")}); err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": "ok"}}, nil
	case "zens":
		r, err := cl.Zens(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: zensFromProto(r.Zens)}, nil
	case "verify":
		r, err := cl.Verify(ctx, &driftnodepb.IdentityReq{Identity: paramString(params, "identity")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"verified": r.Identity}}, nil
	case "unverify":
		r, err := cl.Unverify(ctx, &driftnodepb.IdentityReq{Identity: paramString(params, "identity")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"unverified": r.Identity}}, nil
	case "follow":
		r, err := cl.Follow(ctx, &driftnodepb.FollowReq{Target: paramString(params, "target"), Passphrase: paramString(params, "passphrase")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"followed": r.Followed}}, nil
	case "unfollow":
		r, err := cl.Unfollow(ctx, &driftnodepb.UnfollowReq{Target: paramString(params, "target"), Passphrase: paramString(params, "passphrase")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"unfollowed": r.Unfollowed}}, nil
	case "unlock":
		r, err := cl.Unlock(ctx, &driftnodepb.UnlockReq{Passphrase: paramString(params, "passphrase")})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": r.Status, "identity": r.Identity}}, nil
	case "lock":
		r, err := cl.Lock(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": r.Status}}, nil
	case "sync":
		r, err := cl.Sync(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": r.Status}}, nil
	case "stop":
		r, err := cl.Stop(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"status": r.Status}}, nil
	case "status":
		r, err := cl.Status(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"running": r.Running, "zens": int(r.Zens), "socket": r.Socket, "transport": r.Transport, "unlocked": r.Unlocked}}, nil
	case "rotate-key":
		r, err := cl.RotateKey(ctx, &driftnodepb.Empty{})
		if err != nil {
			return nil, err
		}
		return &Response{Result: map[string]any{"token": r.Token}}, nil
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}

// paramString reads a string parameter from a params map.
func paramString(params map[string]any, key string) string {
	if params == nil {
		return ""
	}
	if v, ok := params[key].(string); ok {
		return v
	}
	return ""
}

// identitiesFromProto converts proto identities back to the map slice shape
// the legacy Response.Result carried.
func identitiesFromProto(ids []*driftnodepb.Identity) []map[string]string {
	out := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, map[string]string{"identity": id.Identity, "name": id.Name})
	}
	return out
}

// feedItemsFromProto converts proto feed items back to the feedItem slice
// shape the legacy Response.Result carried.
func feedItemsFromProto(items []*driftnodepb.FeedItem) []feedItem {
	out := make([]feedItem, 0, len(items))
	for _, it := range items {
		out = append(out, feedItem{
			ID:        it.Id,
			Timestamp: it.Timestamp,
			Author:    it.Author,
			Name:      it.Name,
			Text:      it.Text,
			LikeCount: int(it.LikeCount),
			Likers:    it.Likers,
		})
	}
	return out
}

// zensFromProto converts proto zens back to the []any shape the legacy
// Response.Result carried, so callers asserting on resp.Result.([]any)
// keep working.
func zensFromProto(zens []*driftnodepb.Zen) []any {
	out := make([]any, 0, len(zens))
	for _, z := range zens {
		out = append(out, map[string]any{
			"id":       z.Id,
			"identity": z.Identity,
			"name":     z.Name,
			"kind":     z.Kind,
			"status":   z.Status,
			"verified": z.Verified,
		})
	}
	return out
}
