/*
 * Tencent is pleased to support the open source community by making Blueking Container Service available.
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mysql

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	log "github.com/hashicorp/go-hclog"
	physEtcd "github.com/hashicorp/vault/physical/etcd"
	"github.com/hashicorp/vault/sdk/helper/parseutil"
	"github.com/hashicorp/vault/sdk/physical"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	haLockBackendMySQL = "mysql"
	haLockBackendETCD  = "etcd"
)

// Verify etcdHABackend satisfies the correct interfaces
var (
	_ physical.HABackend = (*etcdHABackend)(nil)
)

type etcdHABackend struct {
	logger         log.Logger
	path           string
	haEnabled      bool
	lockTimeout    time.Duration
	requestTimeout time.Duration
	permitPool     *physical.PermitPool
	etcd           *clientv3.Client
}

// newEtcdHABackend constructs a etcd backend using a given machine address.
func newEtcdHABackend(conf map[string]string, logger log.Logger) (physical.HABackend, error) {
	// ha_etcd_address 不设置默认值, 必须主动设置才开启
	addr := conf["ha_etcd_address"]
	if addr == "" {
		return nil, fmt.Errorf("missing ha_etcd_address")
	}
	endpoints := strings.Split(addr, ",")

	haEnabled := conf["ha_etcd_enabled"]
	if haEnabled == "" {
		haEnabled = "false"
	}
	haEnabledBool, err := strconv.ParseBool(haEnabled)
	if err != nil {
		return nil, fmt.Errorf("value [%v] of 'ha_etcd_enabled' could not be understood", haEnabled)
	}

	// Get the etcd path form the configuration.
	path, ok := conf["ha_etcd_path"]
	if !ok || path == "" {
		path = "/bk_bscp_vault"
	}

	// Ensure path is prefixed.
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	cfg := clientv3.Config{
		Endpoints: endpoints,
	}

	cert, hasCert := conf["ha_etcd_tls_cert_file"]
	key, hasKey := conf["ha_etcd_tls_key_file"]
	ca, hasCa := conf["ha_etcd_tls_ca_file"]
	if (hasCert && hasKey) || hasCa {
		tls := transport.TLSInfo{
			TrustedCAFile: ca,
			CertFile:      cert,
			KeyFile:       key,
		}

		tlscfg, err := tls.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = tlscfg
	}

	etcd, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	sLock := conf["ha_etcd_lock_timeout"]
	if sLock == "" {
		// etcd3 default lease duration is 60s. set to 15s for faster recovery.
		sLock = "15s"
	}
	lock, err := parseutil.ParseDurationSecond(sLock)
	if err != nil {
		return nil, fmt.Errorf("value [%v] of 'ha_etcd_lock_timeout' could not be understood: %w", sLock, err)
	}

	sReqTimeout := conf["ha_etcd_request_timeout"]
	if sReqTimeout == "" {
		// etcd3 default request timeout is set to 5s. It should be long enough
		// for most cases, even with internal retry.
		sReqTimeout = "5s"
	}
	reqTimeout, err := parseutil.ParseDurationSecond(sReqTimeout)
	if err != nil {
		return nil, fmt.Errorf("value [%v] of 'ha_etcd_request_timeout' could not be understood: %w", sReqTimeout, err)
	}

	return &etcdHABackend{
		path:           path,
		etcd:           etcd,
		permitPool:     physical.NewPermitPool(physical.DefaultParallelOperations),
		logger:         logger,
		haEnabled:      haEnabledBool,
		lockTimeout:    lock,
		requestTimeout: reqTimeout,
	}, nil
}

// LockWith implement HABackend interface.
func (c *etcdHABackend) LockWith(key, value string) (physical.Lock, error) {
	p := path.Join(c.path, key)
	return physEtcd.NewEtcdLock(p, value, c.etcd, c.lockTimeout, c.requestTimeout)
}

// HAEnabled implement HABackend interface.
func (c *etcdHABackend) HAEnabled() bool {
	return c.haEnabled
}
