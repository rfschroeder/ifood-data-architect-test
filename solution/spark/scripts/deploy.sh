#!/bin/bash

project=ifood-data-architect-test
app_version=1.0.0
scala_version=2.11
root_path=$(pwd)
target_dir="$root_path/target"
app_name="$project-$app_version.jar"
app_jar="$target_dir/scala-$scala_version/$app_name"
s3_target=s3://ifood-data-architect-resources-renan

function copy_file_to_s3 () {
  source=$1
  target=$2
  aws s3 rm "$target" &&
  aws s3 cp "$source" "$target" --acl public-read
}

# Compile and build app
sbt clean compile && sbt package

if [[ -d $target_dir && -f $app_jar ]]; then
  copy_file_to_s3 "$app_jar" "$s3_target/$app_name"
fi

cd "$root_path" || exit
