/*-----------------------------------------------------------------------

Copyright 2022 PEAK6 INVESTMENTS LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-----------------------------------------------------------------------*/

package mmd

import (
	"flag"
	"os"
	"strconv"
)

var mmdUrl = "localhost:9999"

func init() {
	flag.StringVar(&mmdUrl, "mmd", mmdUrl, "Sets default MMD Url")
	//env takes precedence
	mmdUrl = getEnv("MMD_URL_OVERRIDE", mmdUrl)
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		var result, err = strconv.ParseBool(value)
		if err != nil {
			panic("Invalid value for boolean env var " + key +
				" - must be one of: 1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False")
		}
		return result
	}
	return fallback
}
