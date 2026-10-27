package daemon

import (
	"driftnode/internal/proto/driftnodepb"
)

// feedItemToProto converts one feedItem to a proto FeedItem.
func feedItemToProto(it feedItem) *driftnodepb.FeedItem {
	return &driftnodepb.FeedItem{
		Id:        it.ID,
		Timestamp: it.Timestamp,
		Author:    it.Author,
		Name:      it.Name,
		Text:      it.Text,
		LikeCount: int32(it.LikeCount),
		Likers:    it.Likers,
	}
}

// feedItemsToProto converts a feedItem slice to proto.
func feedItemsToProto(items []feedItem) []*driftnodepb.FeedItem {
	out := make([]*driftnodepb.FeedItem, 0, len(items))
	for _, it := range items {
		out = append(out, feedItemToProto(it))
	}
	return out
}
