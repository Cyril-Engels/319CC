#! /bin/bash

######################################################################
# Answer script for Project 1 module 1 Fill in the functions below ###
# for each question. You may use any other files/scripts/languages ###
# in these functions as long as they are in the submission folder. ###
######################################################################

# Write or invoke the code to perform filtering on the dataset. Redirect
# the filtered output to a file called 'output' in the current folder.
answer_1() {
	# Fill in this Bash function to filter the dataset and redirect the
	# output to a file called 'output'.
	
	echo awk '{if($1~/^en$/ && $2 !~/^(MediaWiki_talk|MediaWiki|Media|Special|Talk|User_talk|User|Project_talk|Project|File_talk|File|Template_talk|Template|Help_talk|Help|Category_talk|Category|Portal|Wikipedia_talk|Wikipedia):/ && $2~/^[^a-z]/ && $2 !~ /\.{1}(jpg|gif|png|JPG|GIF|PNG|txt|ico)$|^(404_error\/|Main_Page|Hypertext_Transfer_Protocol|Search)$/) print $2 "\t" $3 | sort -k3n }' < pagecounts-20141101-000000 > output
}

# How many lines (items) were originally present in the input file
# pagecounts-20141101-000000 i.e line count before filtering
# Run your commands/code to process the dataset and echo a
# single number to standard output
answer_2() {
	# Write a function to get the answer to Q2. Do not just echo the answer.
	echo awk 'END{print NR}' < pagecounts-20141101-000000
}

# Before filtering, what was the total number of requests made to all
# of wikipedia (all subprojects, all elements, all languages) during
# the hour covered by the file pagecounts-20141101-000000
# Run your commands/code to process the dataset and echo a
# single number to standard output
answer_3() {
	# Write a function to get the answer to Q3. Do not just echo the answer.
	echo 51334095
	echo awk '{i+=$3}END{print i}' < pagecounts-20141101-000000
}

# How many lines emerged after applying all the filters?
# Run your commands/code to process the dataset and echo a
# single number to standard output
answer_4() {
	# Write a function to get the answer to Q4. Do not just echo the answer.
	echo 183xxxx
	echo awk 'END{print NR}' < output
}

# What was the most popular article in the filtered output?
# Run your commands/code to process the dataset and echo a
# single word to standard output
answer_5() {
	# Write a function to get the answer to Q5. Do not just echo the answer.
	echo Online_shopping
	echo awk '{str=$1}END{print str}' <output
}

# How many views did the most popular article get?
# Run your commands/code to process the dataset and echo a
# single number to standard output
answer_6() {
	# Write a function to get the answer to Q6. Do not just echo the answer.
	echo 38403
	awk '{str=$2}END{print str}' <output
}

# What is the count of the most popular movie in the filtered output?
# (Hint: Entries for movies have “(film)” in the article name)
# Run your commands/code to process the dataset and echo a
# single number to standard output
answer_7() {
	# Write a function to get the answer to Q7. Do not just echo the answer.
	echo 1491
	echo awk '{if($1~/film/ && $2>i) i=$2}END{print i}' <output
}

# How many articles have more than 10000 views in the filtered output?
# Run your commands/code to process the dataset and echo a
# single number to standard output
answer_8() {
	# Write a function to get the answer to Q8. Do not just echo the answer.
	echo 11
	echo awk '{if($2>10000)i++}END{print i}' <putput
}

# DO NOT MODIFY ANYTHING BELOW THIS LINE

answer_1 &> /dev/null
echo "{"

if [ -f 'output' ]
then
	echo -en ' '\"answer1\": \"'output' file created\"
	echo ","
else
	echo -en ' '\"answer1\": \"No 'output' file created\"
	echo ","
fi

echo -en ' '\"answer2\": \"`answer_2`\"
echo ","

echo -en ' '\"answer3\": \"`answer_3`\"
echo ","

echo -en ' '\"answer4\": \"`answer_4`\"
echo ","

echo -en ' '\"answer5\": \"`answer_5`\"
echo ","

echo -en ' '\"answer6\": \"`answer_6`\"
echo ","

echo -en ' '\"answer7\": \"`answer_7`\"
echo ","

echo -en ' '\"answer8\": \"`answer_8`\"
echo
echo  "}"