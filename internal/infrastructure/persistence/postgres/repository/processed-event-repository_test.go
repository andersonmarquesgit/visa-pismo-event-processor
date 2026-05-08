package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/persistence/postgres/model"
)

func TestProcessedEventRepository_Save(t *testing.T) {
	t.Parallel()

	t.Run("nil event returns error", func(t *testing.T) {
		t.Parallel()

		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock new: %v", err)
		}
		defer db.Close()

		repo := NewProcessedEventRepository(db)
		if err := repo.Save(context.Background(), nil); err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("uses defaults for status/timestamps", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock new: %v", err)
		}
		defer db.Close()

		mock.ExpectExec("INSERT INTO processed_events").
			WithArgs(
				"id-1",
				"event-1",
				"t.type",
				"tenant-1",
				[]byte(`{"x":1}`),
				processedEventStatusProcessed,
				sqlmock.AnyArg(), // processed_at defaulted
				sqlmock.AnyArg(), // created_at defaulted
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		repo := NewProcessedEventRepository(db)
		err = repo.Save(context.Background(), &model.ProcessedEvent{
			ID:        "id-1",
			EventID:   "event-1",
			EventType: "t.type",
			TenantID:  "tenant-1",
			Payload:   []byte(`{"x":1}`),
		})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("expectations: %v", err)
		}
	})

	t.Run("uses provided status/timestamps", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock new: %v", err)
		}
		defer db.Close()

		processedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		createdAt := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)

		mock.ExpectExec("INSERT INTO processed_events").
			WithArgs(
				"id-2",
				"event-2",
				"t2.type",
				"tenant-2",
				[]byte(`{"y":2}`),
				"processed",
				processedAt,
				createdAt,
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		repo := NewProcessedEventRepository(db)
		err = repo.Save(context.Background(), &model.ProcessedEvent{
			ID:          "id-2",
			EventID:     "event-2",
			EventType:   "t2.type",
			TenantID:    "tenant-2",
			Payload:     []byte(`{"y":2}`),
			Status:      "processed",
			ProcessedAt: processedAt,
			CreatedAt:   createdAt,
		})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("expectations: %v", err)
		}
	})
}

