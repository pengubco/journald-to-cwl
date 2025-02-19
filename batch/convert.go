package batch

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/coreos/go-systemd/v22/sdjournal"
)

// EntryToEventConverter convertes journal entry to log event.
type EntryToEventConverter func(e *sdjournal.JournalEntry) types.InputLogEvent

// NewEntryToEventConverter returns a converter that adds instanceID to the entry, and uses the given timestampFn for
// the CWL log event timestamp.
func NewEntryToEventConverter(instanceID string, timestampFn func() time.Time) EntryToEventConverter {
	return func(e *sdjournal.JournalEntry) types.InputLogEvent {
		r := recordFromJournalEntryFields(e)
		r.InstanceID = instanceID

		event := types.InputLogEvent{
			// Use the timestamp of reading the entry to keep the existing behavior. The timestamp of the entry can be
			// found in realTimestamp of the log.
			Timestamp: aws.Int64(timestampFn().UnixMilli()),
		}
		// Indent to keep the existing behavior.
		jsonDataBytes, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			event.Message = aws.String(fmt.Sprintf("cannot marshal record, %s", err))
			return event
		}
		event.Message = aws.String(string(jsonDataBytes))
		return event
	}
}

// priorityMap maps the integer priority level to human readable level. The map is the same as levels from
// `man` journalctl`.
var priorityMap = map[string]string{
	"0": "emerg",
	"1": "alert",
	"2": "crit",
	"3": "err",
	"4": "warning",
	"5": "notice",
	"6": "info",
	"7": "debug",
}

// Record corresponds to a CWL event. It contains instance-id and fields from journal entry.
// For common fields, refer https://www.freedesktop.org/software/systemd/man/latest/systemd.journal-fields.html.
type Record struct {
	InstanceID        string       `json:"instanceId,omitempty"`
	RealtimeTimestamp uint64       `json:"realTimestamp,omitempty"`
	PID               int          `json:"pid"`
	UID               int          `json:"uid"`
	GID               int          `json:"gid"`
	Command           string       `json:"cmdName,omitempty"`
	Executable        string       `json:"exe,omitempty"`
	SystemdUnit       string       `json:"systemdUnit,omitempty"`
	BootID            string       `json:"bootId,omitempty"`
	MachineID         string       `json:"machineId,omitempty"`
	Hostname          string       `json:"hostname,omitempty"`
	Transport         string       `json:"transport,omitempty"`
	Priority          string       `json:"priority,omitempty"`
	Message           string       `json:"message,omitempty"`
	MessageID         string       `json:"messageId,omitempty"`
	ErrNo             int          `json:"errNo,omitempty"`
	Syslog            RecordSyslog `json:"syslog,omitempty"`
}

type RecordSyslog struct {
	Facility   int    `json:"facility,omitempty"`
	Identifier string `json:"ident,omitempty"`
	PID        int    `json:"pid,omitempty"`
}

// recordFromJournalEntryFields fills a Record with fields from a journal entry.
func recordFromJournalEntryFields(e *sdjournal.JournalEntry) *Record {
	var r Record
	r.RealtimeTimestamp = e.RealtimeTimestamp
	f := e.Fields
	if pid, err := strconv.Atoi(f["_PID"]); err == nil {
		r.PID = pid
	}
	if uid, err := strconv.Atoi(f["_UID"]); err == nil {
		r.UID = uid
	}
	if gid, err := strconv.Atoi(f["_GID"]); err == nil {
		r.GID = gid
	}
	if errNo, err := strconv.Atoi(f["ERRNO"]); err == nil {
		r.ErrNo = errNo
	}
	r.Command = f["_COMM"]
	r.Executable = f["_EXE"]
	r.SystemdUnit = f["_SYSTEMD_UNIT"]
	r.BootID = f["_BOOT_ID"]
	r.MachineID = f["_MACHINE_ID"]
	r.Hostname = f["_HOSTNAME"]
	r.Transport = f["_TRANSPORT"]
	r.Priority = priorityMap[f["PRIORITY"]]
	r.Message = f["MESSAGE"]
	r.MessageID = f["MESSAGE_ID"]

	if facility, err := strconv.Atoi(f["SYSLOG_FACILITY"]); err == nil {
		r.Syslog.Facility = facility
	}
	if pid, err := strconv.Atoi(f["SYSLOG_PID"]); err == nil {
		r.Syslog.PID = pid
	}
	r.Syslog.Identifier = f["SYSLOG_IDENTIFIER"]
	return &r
}
