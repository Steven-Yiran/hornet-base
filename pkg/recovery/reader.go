package recovery

import (
	"bytes"
	"io"

	uuid "github.com/google/uuid"
	backscanner "github.com/icza/backscanner"
)

// Helper method that gets all log strings and most recent checkpoint position from the log file.
func (rm *RecoveryManager) getRelevantStrings() (
	relevantStrings []string, checkpointPos int, err error) {
	fstats, err := rm.fd.Stat()
	if err != nil {
		return nil, 0, err
	}

	scanner := backscanner.New(rm.fd, int(fstats.Size()))
	checkpointTarget := []byte("checkpoint")
	startTarget := []byte("start")
	relevantStrings = make([]string, 0)
	checkpointHit := false
	txs := make(map[uuid.UUID]bool)
	for {
		line, _, err := scanner.LineBytes()
		if err != nil {
			if err == io.EOF {
				return relevantStrings, 0, nil
			} else {
				return nil, 0, err
			}
		}
		relevantStrings = append([]string{string(line)}, relevantStrings...)
		checkpointPos += 1
		if checkpointHit {
			if bytes.Contains(line, startTarget) {
				log, err := FromString(string(line))
				if err != nil {
					return nil, 0, err
				}
				id := log.(*startLog).id
				delete(txs, id)
			}
		}
		if !checkpointHit && bytes.Contains(line, checkpointTarget) {
			checkpointHit = true
			log, err := FromString(string(line))
			if err != nil {
				return nil, 0, err
			}
			for _, tx := range log.(*checkpointLog).ids {
				txs[tx] = true
			}
			checkpointPos = 0
		}
		if checkpointHit && len(txs) <= 0 {
			break
		}
	}
	return relevantStrings, checkpointPos, err
}

// Reads in the logs and most recent checkpoint position from disk.
func (rm *RecoveryManager) readLogs() (
	logs []Log, checkpointPos int, err error) {
	strings, checkpointPos, err := rm.getRelevantStrings()
	if err != nil {
		return nil, 0, err
	}
	if len(strings) > 0 {
		logs = make([]Log, len(strings)-1)
		for i, s := range strings[:len(strings)-1] {
			log, err := FromString(s)
			if err != nil {
				return nil, 0, err
			}
			logs[i] = log
		}
	} else {
		logs = make([]Log, 0)
	}
	return logs, checkpointPos, nil
}
