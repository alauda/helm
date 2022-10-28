/*
Copyright The Helm Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package chartutil

import (
	"github.com/Masterminds/semver/v3"
	"regexp"
)

// IsCompatibleRange compares a version to a constraint.
// It returns true if the version matches the constraint, and false in all other cases.
func IsCompatibleRange(constraint, ver string) bool {
	// ALAUDA-FIX: support eks version
	r, _ := regexp.Compile(`v[\d]\.[\d]*\.[\d]*`)
	nv := r.FindString(ver)

	sv, err := semver.NewVersion(nv)
	if err != nil {
		return false
	}

	c, err := semver.NewConstraint(constraint)
	if err != nil {
		return false
	}
	return c.Check(sv)
}
