package memstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestPrincipalCannotReachAnotherPrincipal is the PR 5 gate from the plan:
// "principal A cannot search, fetch, scroll, count, infer, or reuse a cursor
// for principal B."
//
// The existing tests cover each vector separately and in passing. This one
// exists because the gate is a single claim about a set of operations, and a
// set is not verified by testing its members in different files at different
// times: a new read path added later can be unscoped without any of those
// tests noticing. Every vector is asserted here against the same two-principal
// archive, so adding a sixth vector means adding a case to this test.
//
// The archive holds one turn for "owner" and one for "intruder" in the same
// adapter. Both principals are real, so a leak is a leak and not an empty
// result that would pass for the wrong reason.
func TestPrincipalCannotReachAnotherPrincipal(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	// Two principals, same adapter, same project name. The shared project name
	// is deliberate: a filter that narrows by project but forgets the principal
	// would still return the other user's row, and a fixture with distinct
	// project names would hide that.
	for _, spec := range []turnSpec{
		{
			Delivery: "gate-owner", Project: "shared", WorkDir: "/work/shared",
			Request: "owner secret marker alpha",
			Answer:  "owner answer marker alpha",
			Created: 1700000000000,
		},
		{
			Delivery: "gate-intruder", Project: "shared", WorkDir: "/work/shared",
			Request: "intruder secret marker beta",
			Answer:  "intruder answer marker beta",
			Created: 1700000100000,
			UserID:  "intruder",
		},
	} {
		recordTurn(t, writer, spec)
	}
	indexAll(t, writer)

	owner := SearchScope{Adapter: "test", UserID: "owner"}
	intruder := SearchScope{Adapter: "test", UserID: "intruder"}

	// Both principals must be able to find their own marker. Without this the
	// test would pass on an archive that indexed nothing.
	ownerHits, err := writer.store.Search(ctx, SearchOptions{Scope: owner, Query: "marker"})
	if err != nil {
		t.Fatalf("owner search: %v", err)
	}
	if len(ownerHits.Hits) == 0 {
		t.Fatal("owner cannot find its own marker; the fixture is empty and the gate would pass vacuously")
	}
	intruderHits, err := writer.store.Search(ctx, SearchOptions{Scope: intruder, Query: "marker"})
	if err != nil {
		t.Fatalf("intruder search: %v", err)
	}
	if len(intruderHits.Hits) == 0 {
		t.Fatal("intruder cannot find its own marker; the fixture is empty and the gate would pass vacuously")
	}

	// Collect the IDs each principal legitimately owns, so the fetch and scroll
	// vectors below attack real records rather than invented ones.
	ownerIDs := hitMessageIDs(ownerHits.Hits)
	intruderIDs := hitMessageIDs(intruderHits.Hits)
	ownerSession := ownerHits.Hits[0].SessionID
	intruderSession := intruderHits.Hits[0].SessionID

	if ownerSession == intruderSession {
		t.Fatal("both principals resolved to the same session; the fixture cannot distinguish them")
	}

	t.Run("search", func(t *testing.T) {
		// The intruder's own search must not contain any of the owner's text.
		for _, hit := range intruderHits.Hits {
			if strings.Contains(hit.Excerpt, "alpha") {
				t.Fatalf("intruder search leaked owner excerpt %q", hit.Excerpt)
			}
		}
		// A query for the owner's exact marker must return nothing to the
		// intruder. This is the "infer" vector: even a query that names the
		// other principal's content must not confirm it exists.
		leak, err := writer.store.Search(ctx, SearchOptions{Scope: intruder, Query: "alpha"})
		if err != nil {
			t.Fatalf("intruder search for owner marker: %v", err)
		}
		if len(leak.Hits) != 0 {
			t.Fatalf("intruder search for the owner's marker returned %d hits", len(leak.Hits))
		}
	})

	t.Run("fetch", func(t *testing.T) {
		// Fetching the owner's message IDs under the intruder's scope must not
		// return the body. The plan requires a non-disclosing unavailable
		// result rather than an error that confirms the ID exists.
		result, err := writer.store.GetMessages(ctx, intruder, ownerIDs, 0)
		if err != nil {
			t.Fatalf("cross-principal fetch errored instead of returning unavailable: %v", err)
		}
		for _, detail := range result.Messages {
			if strings.Contains(detail.Text, "alpha") {
				t.Fatalf("cross-principal fetch leaked owner body %q", detail.Text)
			}
		}
		if len(result.Messages) != 0 {
			t.Fatalf("cross-principal fetch returned %d messages", len(result.Messages))
		}
		// The owner must still be able to fetch the same IDs, or the test is
		// measuring a broken fetch rather than a working boundary.
		own, err := writer.store.GetMessages(ctx, owner, ownerIDs, 0)
		if err != nil {
			t.Fatalf("owner fetch: %v", err)
		}
		if len(own.Messages) == 0 {
			t.Fatal("owner cannot fetch its own messages; the fetch vector is vacuous")
		}
	})

	t.Run("scroll", func(t *testing.T) {
		// Scrolling the owner's session under the intruder's scope must fail.
		if _, err := writer.store.Scroll(ctx, intruder, ownerSession, "", 5); err == nil {
			t.Fatal("cross-principal scroll was accepted")
		}
		// And the owner must be able to scroll it, so the failure above is the
		// boundary and not a broken scroll.
		if _, err := writer.store.Scroll(ctx, owner, ownerSession, "", 5); err != nil {
			t.Fatalf("owner cannot scroll its own session: %v", err)
		}
	})

	t.Run("count", func(t *testing.T) {
		// The legacy aggregate paths must not widen to every user when the
		// principal is empty, and must not cross principals when it is set.
		// These two functions used to skip the user filter entirely on an empty
		// userID, which returned every user's records.
		if _, err := writer.store.Recent("test", "", 0, 10); !errors.Is(err, ErrSearchScopeRequired) {
			t.Fatalf("Recent with an empty user was not rejected: %v", err)
		}
		if _, err := writer.store.ProjectSummaries("", "test", time.Time{}, 1); !errors.Is(err, ErrSearchScopeRequired) {
			t.Fatalf("ProjectSummaries with an empty user was not rejected: %v", err)
		}
		// A scoped call must return only that principal's records.
		records, err := writer.store.Recent("test", "intruder", 0, 50)
		if err != nil {
			t.Fatalf("intruder Recent: %v", err)
		}
		for _, record := range records {
			if record.UserID != "intruder" {
				t.Fatalf("Recent leaked a record for %q", record.UserID)
			}
		}
		projects, err := writer.store.ProjectSummaries("intruder", "test", time.Time{}, 1)
		if err != nil {
			t.Fatalf("intruder ProjectSummaries: %v", err)
		}
		for _, project := range projects {
			if project.UserID != "intruder" {
				t.Fatalf("ProjectSummaries leaked a project for %q", project.UserID)
			}
		}
	})

	t.Run("cursor", func(t *testing.T) {
		// A cursor issued to one principal must not be usable by another. The
		// cursor carries a fingerprint of the scope it was issued under, so
		// reuse is rejected rather than silently re-anchored.
		page, err := writer.store.Search(ctx, SearchOptions{Scope: owner, Query: "marker", Limit: 1})
		if err != nil {
			t.Fatalf("owner paged search: %v", err)
		}
		if page.NextCursor == "" {
			t.Skip("owner search did not truncate, so there is no cursor to reuse")
		}
		if _, err := writer.store.Search(ctx, SearchOptions{
			Scope: intruder, Query: "marker", Limit: 1, Cursor: page.NextCursor,
		}); !errors.Is(err, ErrSearchRestartRequired) {
			t.Fatalf("cross-principal cursor reuse was accepted: %v", err)
		}
	})

	// The IDs the intruder legitimately owns must never appear in the owner's
	// results either. A boundary that only holds in one direction is not a
	// boundary.
	for _, id := range intruderIDs {
		for _, ownerID := range ownerIDs {
			if id == ownerID {
				t.Fatalf("both principals own message %q", id)
			}
		}
	}
}

func hitMessageIDs(hits []SearchHit) []string {
	ids := make([]string, 0, len(hits))
	for _, hit := range hits {
		ids = append(ids, hit.MessageID)
	}
	return ids
}
