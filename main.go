package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/bluebreezecf/opentsdb-goclient/client"
	"github.com/bluebreezecf/opentsdb-goclient/config"
	"github.com/sensu/sensu-go/types"
	"github.com/spf13/cobra"
)

var (
	addr  string
	stdin *os.File
)

func main() {
	rootCmd := configureRootCommand()
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err.Error())
	}
}

func configureRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sensu-opentsdb-handler",
		Short: "an opentsdb handler built for use with sensu",
		RunE:  run,
	}

	cmd.Flags().StringVarP(&addr,
		"addr",
		"a",
		"",
		"the address of the opentsdb server, should be of the form 'host:port'")

	_ = cmd.MarkFlagRequired("addr")

	return cmd
}

func run(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		_ = cmd.Help()
		return errors.New("invalid argument(s) received")
	}

	if stdin == nil {
		stdin = os.Stdin
	}

	eventJSON, err := ioutil.ReadAll(stdin)
	if err != nil {
		return fmt.Errorf("failed to read stdin: %s", err.Error())
	}

	event := &types.Event{}
	err = json.Unmarshal(eventJSON, event)
	if err != nil {
		return fmt.Errorf("failed to unmarshal stdin data: %s", err.Error())
	}

	if err = event.Validate(); err != nil {
		return fmt.Errorf("failed to validate event: %s", err.Error())
	}

	if !event.HasMetrics() {
		return fmt.Errorf("event does not contain metrics")
	}

	return sendMetrics(event)
}

func sendMetrics(event *types.Event) error {
	fmt.Println(addr)
	opentsdbCfg := config.OpenTSDBConfig{
		OpentsdbHost: addr,
	}
	tsdbClient, err := client.NewClient(opentsdbCfg)
	if err != nil {
		fmt.Printf("%v\n", err)
		return err
	}

	//0. Ping
	if err = tsdbClient.Ping(); err != nil {
		fmt.Println(err.Error())
		return err
	}
	dataPoints := make([]client.DataPoint, 0)
	for _, point := range event.Metrics.Points {
		nameField := strings.Split(point.Name, ".")
		name := nameField[0]
		stringTimestamp := strconv.FormatInt(point.Timestamp, 10)
		if len(stringTimestamp) > 10 {
			stringTimestamp = stringTimestamp[:10]
		}
		tags := make(map[string]string)
		tags["sensu_entity_id"] = event.Entity.ObjectMeta.Name
		for _, tag := range point.Tags {
			tags[tag.Name] = tag.Value
		}
		data := client.DataPoint{
			Metric:    name,
			Timestamp: point.Timestamp,
			Value:     point.Value,
		}
		data.Tags = tags
		dataPoints = append(dataPoints, data)
	}

	resp, err := tsdbClient.Put(dataPoints, "details")
	if err != nil {
		return err
	}
	log.Print(resp)
	return nil
}
