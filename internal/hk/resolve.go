package hk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strconv"

	"github.com/mgh3326/recallbeam/internal/core"
)

// resolve uses complete paginated identity scans when the source has no global
// row lookup. This is deliberately uncapped: a truncated list is not evidence
// that a source row was deleted. Global row reads avoid this expensive path.
func (c Client) resolve(ctx context.Context, model string, ref core.Ref) (core.Doc, error) {
	var found core.Doc
	matched := false
	scanModel := "task"
	if model == "document" {
		scanModel = model
	}
	err := c.eachRef(ctx, scanModel, func(r core.Ref) (bool, error) {
		if model == "document" {
			if r.ID != ref.ID {
				return false, nil
			}
			var err error
			found, err = c.Fetch(ctx, model, r)
			matched = err == nil
			return true, err
		}
		if model == "task_comment" {
			id, err := strconv.ParseInt(ref.ID, 10, 64)
			if err != nil || id < 1 {
				return false, errors.New("invalid comment identity")
			}
			response, err := c.get(ctx, "/v1/tasks/"+url.PathEscape(r.ID)+"/comments?after_id="+strconv.FormatInt(id-1, 10)+"&limit=1")
			if errors.Is(err, core.ErrNotFound) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			var list struct {
				Comments []struct {
					ID        int64           `json:"id"`
					Body      string          `json:"body"`
					CreatedAt json.RawMessage `json:"created_at"`
				} `json:"comments"`
			}
			err = json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&list)
			response.Body.Close()
			if err != nil {
				return false, err
			}
			if len(list.Comments) == 0 || list.Comments[0].ID != id {
				return false, nil
			}
			found.Body = list.Comments[0].Body
			err = json.Unmarshal(list.Comments[0].CreatedAt, &found.UpdatedAt)
			matched = err == nil
			return true, err
		}
		if model == "decision" {
			response, err := c.get(ctx, "/v1/tasks/"+url.PathEscape(r.ID))
			if errors.Is(err, core.ErrNotFound) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			var task struct {
				Refs struct {
					Decision json.RawMessage `json:"decision_request"`
				} `json:"refs"`
			}
			err = json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&task)
			response.Body.Close()
			if err != nil {
				return false, err
			}
			var decision struct {
				ID, Question string
			}
			if len(task.Refs.Decision) == 0 {
				return false, nil
			}
			if err = json.Unmarshal(task.Refs.Decision, &decision); err != nil {
				return false, err
			}
			if decision.ID != ref.ID {
				return false, nil
			}
			found.Title = decision.Question
			found.Body = string(task.Refs.Decision)
			found.UpdatedAt = r.UpdatedAt
			matched = true
			return true, nil
		}
		return false, errors.New("unsupported lookup model")
	})
	if err != nil {
		return core.Doc{}, err
	}
	if !matched {
		return core.Doc{}, core.ErrNotFound
	}
	return found, nil
}
func (c Client) eachRef(ctx context.Context, model string, visit func(core.Ref) (bool, error)) error {
	cursor := ""
	seen := map[string]bool{}
	schema := 0
	for {
		p, err := c.Bootstrap(ctx, model, cursor)
		if err != nil {
			return err
		}
		if p.Schema < 1 {
			return errors.New("invalid bootstrap metadata")
		}
		if schema != 0 && schema != p.Schema {
			return core.ErrReset
		}
		schema = p.Schema
		for _, r := range p.Rows {
			done, err := visit(r)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
		if p.Next == "" {
			return nil
		}
		if seen[p.Next] {
			return errors.New("repeated bootstrap cursor")
		}
		seen[p.Next] = true
		cursor = p.Next
	}
}
