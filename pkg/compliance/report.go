package compliance

import (
	"bufio"
	"encoding/json"
	"os"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
)

type EventSummary struct {
	Total        int            `json:"total"`
	Success      int            `json:"success"`
	Failure      int            `json:"failure"`
	ByEventType  map[string]int `json:"by_event_type"`
	ByStatusCode map[string]int `json:"by_status_code"`
	From         time.Time      `json:"from"`
	To           time.Time      `json:"to"`
}

type HIPAAReport struct {
	GeneratedAt time.Time    `json:"generated_at"`
	Summary     EventSummary `json:"summary"`
}

type SOC2Report struct {
	GeneratedAt time.Time    `json:"generated_at"`
	Summary     EventSummary `json:"summary"`
}

type ComplianceReporter struct {
	auditPath string
}

func NewComplianceReporter(auditPath string) *ComplianceReporter {
	return &ComplianceReporter{auditPath: auditPath}
}

func (c *ComplianceReporter) ExportHIPAAEvidence(startDate, endDate time.Time) (*HIPAAReport, error) {
	sum, err := c.readSummary(startDate, endDate)
	if err != nil {
		return nil, err
	}
	return &HIPAAReport{GeneratedAt: time.Now().UTC(), Summary: sum}, nil
}

func (c *ComplianceReporter) ExportSOC2Evidence(startDate, endDate time.Time) (*SOC2Report, error) {
	sum, err := c.readSummary(startDate, endDate)
	if err != nil {
		return nil, err
	}
	return &SOC2Report{GeneratedAt: time.Now().UTC(), Summary: sum}, nil
}

func (c *ComplianceReporter) readSummary(startDate, endDate time.Time) (EventSummary, error) {
	f, err := os.Open(c.auditPath)
	if err != nil {
		return EventSummary{}, err
	}
	defer f.Close()

	sum := EventSummary{
		ByEventType:  map[string]int{},
		ByStatusCode: map[string]int{},
		From:         startDate,
		To:           endDate,
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var ev kms.AuditEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}
		if !startDate.IsZero() && ev.Timestamp.Before(startDate) {
			continue
		}
		if !endDate.IsZero() && ev.Timestamp.After(endDate) {
			continue
		}
		sum.Total++
		sum.ByEventType[ev.EventType]++
		if ev.Status == "SUCCESS" {
			sum.Success++
		} else {
			sum.Failure++
		}
		if ev.ErrorCode != "" {
			sum.ByStatusCode[ev.ErrorCode]++
		}
	}
	return sum, scanner.Err()
}
