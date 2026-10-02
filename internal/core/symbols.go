package core

// Glyphs used to render likes, pins, and verifications across the CLI and TUI.
// Keeping them as constants lets every render path reference one source of
// truth instead of scattering literal runes through the codebase.
const (
	// GlyphLike marks a liked post.
	GlyphLike = "💟"
	// GlyphPin marks a pinned follow.
	GlyphPin = "✨"
	// GlyphVerified marks a verified identity.
	GlyphVerified = "🪪"
)
