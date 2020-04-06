/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.spi.security;

import java.util.Objects;

public class PrivilegeInfo
{
    private final Privilege privilege;
    private final boolean grantOption;

    public PrivilegeInfo(Privilege privilege, boolean grantOption)
    {
        this.privilege = privilege;
        this.grantOption = grantOption;
    }

    public Privilege getPrivilege()
    {
        return privilege;
    }

    public boolean isGrantOption()
    {
        return grantOption;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(privilege, grantOption);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrivilegeInfo privilegeInfo = (PrivilegeInfo) o;
        return privilege == privilegeInfo.privilege &&
                Objects.equals(grantOption, privilegeInfo.grantOption);
    }
}
