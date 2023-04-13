#!/bin/bash

directory_path=$1
save=$2
mapfile -t files < <(ls "$directory_path")

json_object="{"

for file in "${files[@]}"
do
    # Get the number of lines in the file.
    lines=$(wc -l "$directory_path/$file" | awk '{print $1}')

    # Add the file name and number of lines to the JSON object.
    json_object+="\"$file\":$lines,"

    echo "#Lines in $file = $lines"
done

json_object=${json_object%?}
json_object+="}"

echo $json_object > "$save"
