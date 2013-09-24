#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

if [ $# -ne 2 ]
then
  echo "Arguments: <src store> <target store>"
fi

hadoop fs -ls $2
if [ $? -eq 0 ] ; then
  echo "Target $2 exists. Delete and continue"
  exit 0
fi

function migrate() {
SRC=$1
TARGET=$2
rmdir $HOME/migrate-backup
mkdir $HOME/migrate-backup
hadoop fs -mkdir $TARGET

echo "$1 $2"
for entity in CLUSTER FEED PROCESS; do
  first=1
  hadoop fs -mkdir $TARGET/$entity
  mkdir $HOME/migrate-backup/$entity
  for path in `hadoop fs -ls $SRC/$entity | rev | cut -d' ' -f 1 | rev`; do
    if [[ $first -eq 0 ]]; then
      echo "Migrating $path"
      file=`echo $path | rev | cut -d'/' -f1 | rev`
      hadoop fs -cat $path | sed -e "s/ivory/falcon/" > $HOME/migrate-backup/$entity/$file
      hadoop fs -put $HOME/migrate-backup/$entity/$file $TARGET/$entity/$file
    fi
    first=0
  done
done
rmdir $HOME/migrate-backup
}

migrate $1 $2
migrate $1/archive $2/archive
