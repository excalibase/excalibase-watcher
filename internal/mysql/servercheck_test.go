package mysql

import (
	"errors"
	"strings"
	"testing"
)

func goodSettings() serverSettings {
	return serverSettings{LogBin: "1", BinlogFormat: "ROW", RowImage: "FULL", GTIDMode: "ON", EnforceGTID: "ON"}
}

var replicationGrants = []string{
	"GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `cdc`@`%`",
}

func TestCheckServerAcceptsTheMOCODefaults(t *testing.T) {
	if err := checkServer(goodSettings(), replicationGrants, "shop", []string{"orders"}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckServerNamesEverySettingThatIsWrong(t *testing.T) {
	cases := map[string]struct {
		mutate func(*serverSettings)
		want   string
	}{
		"binlog off":     {func(s *serverSettings) { s.LogBin = "0" }, "log_bin"},
		"mixed format":   {func(s *serverSettings) { s.BinlogFormat = "MIXED" }, "binlog_format"},
		"minimal image":  {func(s *serverSettings) { s.RowImage = "MINIMAL" }, "binlog_row_image"},
		"gtid off":       {func(s *serverSettings) { s.GTIDMode = "OFF" }, "gtid_mode"},
		"gtid permitted": {func(s *serverSettings) { s.GTIDMode = "ON_PERMISSIVE" }, "gtid_mode"},
		"no consistency": {func(s *serverSettings) { s.EnforceGTID = "OFF" }, "enforce_gtid_consistency"},
		"warn only":      {func(s *serverSettings) { s.EnforceGTID = "WARN" }, "enforce_gtid_consistency"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			settings := goodSettings()
			tc.mutate(&settings)
			err := checkServer(settings, replicationGrants, "shop", nil)
			if !errors.Is(err, ErrServerNotReady) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ErrServerNotReady naming %s", err, tc.want)
			}
		})
	}
}

func TestCheckServerReportsAllProblemsAtOnce(t *testing.T) {
	settings := goodSettings()
	settings.BinlogFormat = "STATEMENT"
	settings.GTIDMode = "OFF"
	err := checkServer(settings, []string{"GRANT USAGE ON *.* TO `cdc`@`%`"}, "shop", nil)
	for _, want := range []string{"binlog_format", "gtid_mode", "REPLICATION SLAVE", "REPLICATION CLIENT", "SELECT"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want mention of %s", err, want)
		}
	}
}

func TestCheckServerGrantShapes(t *testing.T) {
	cases := map[string]struct {
		grants []string
		tables []string
		ok     bool
	}{
		"all privileges": {[]string{"GRANT ALL PRIVILEGES ON *.* TO `r`@`%`"}, nil, true},
		"select on schema + replication": {[]string{
			"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `r`@`%`",
			"GRANT SELECT ON `shop`.* TO `r`@`%`"}, nil, true},
		"escaped underscore schema": {[]string{
			"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `r`@`%`",
			"GRANT SELECT ON `shop\\_db`.* TO `r`@`%`"}, nil, true},
		"select on every listed table": {[]string{
			"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `r`@`%`",
			"GRANT SELECT ON `shop`.`orders` TO `r`@`%`",
			"GRANT SELECT ON `shop`.`items` TO `r`@`%`"}, []string{"orders", "items"}, true},
		"select on one of two tables": {[]string{
			"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `r`@`%`",
			"GRANT SELECT ON `shop`.`orders` TO `r`@`%`"}, []string{"orders", "items"}, false},
		"select on another schema": {[]string{
			"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `r`@`%`",
			"GRANT SELECT ON `other`.* TO `r`@`%`"}, nil, false},
		"missing replication client": {[]string{"GRANT SELECT, REPLICATION SLAVE ON *.* TO `r`@`%`"}, nil, false},
		"missing replication slave":  {[]string{"GRANT SELECT, REPLICATION CLIENT ON *.* TO `r`@`%`"}, nil, false},
		"grant option does not hide": {[]string{
			"GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `r`@`%` WITH GRANT OPTION"}, nil, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			schemaName := "shop"
			if name == "escaped underscore schema" {
				schemaName = "shop_db"
			}
			err := checkServer(goodSettings(), tc.grants, schemaName, tc.tables)
			if tc.ok != (err == nil) {
				t.Fatalf("ok = %v, err = %v", tc.ok, err)
			}
		})
	}
}
