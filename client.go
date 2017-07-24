package influx

import (
	"github.com/influxdata/influxdb/client/v2"
)

var gClient client.Client

func InitClient(addr string) error {
	var err error
	gClient, err = client.NewHTTPClient(client.HTTPConfig{Addr: addr})
	return err
}

func queryDB(cli client.Client, db string, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := cli.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func Query(db string, cmd string) ([]client.Result, error) {
	return queryDB(gClient, db, cmd)
}

func Insert(db string, point *client.Point) error {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db,
		Precision: "s",
	})
	if err != nil {
		return err
	}
	bp.AddPoint(point)
	return gClient.Write(bp)
}

func WriteBatchPoints(bp client.BatchPoints) error {
	return gClient.Write(bp)
}
