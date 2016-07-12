#! /bin/bash

######################################################################
# Answer script for Project 1 module 2 Fill in the functions below ###
# for each question. You may use any other files/scripts/languages ###
# in these functions as long as they are in the submission folder. ###
######################################################################

# Make sure you have merged the results from the mapreduce job into a file called 'output'
# Please maintain the format of the output mentioned in the P1.2 writeup.
answer_1() {
# Leave this function empty. This function is to check if you have
# merged the results into a file called 'output'
echo 
}

# How many lines emerged in your output files?
# Run your commands/code to process the output file and echo a
# single number to standard output
answer_2() {
# Write a function to get the answer to Q2. Do not just echo the answer.
echo awk 'END{print NR}' < output
}

# What was the most popular article in the filtered output?
# Run your commands/code to process the output and echo a single word
# to standard output
answer_3() {
# Write a function to get the answer to Q3. Do not just echo the answer.
echo awk '{if($1>i) i=$1 str=$2}END{print str}' < output
}

# How many total views did the most popular article get over the month?
# Run your commands/code to process the output and echo a
# single number to standard output
answer_4() {
# Write a function to get the answer to Q4. Do not just echo the answer.
echo awk '{if($1>i) i=$1}END{print i}' < output
}

# For how many days over the month was the page titled "Google" more popular
# than the page titled "Amazon.com" ?
# Run your commands/code to process the dataset and echo a single number to standard output
# Do not hardcode the articles, as we will run your code with different articles as input
# For your convenience, "Google" is stored in the variable 'first', and "Amazon.com" in 'second'.
answer_5() {
# Write a function to get the answer to Q5. Do not just echo the answer.
first=$(head -n 1 q5) #Google
second=$(cat q5 | sed -n 2p) #Amazon.com
python q5.py $first $second
echo 
}

# Rank the Movie titles in the file q6 based on their single-day maximum wikipedia page views
# (In descending order of page views, with the highest one first):
# Begin_Again_(film), Annabelle_(film), Predestination_(film), The_Fault_in_Our_Stars_(film), The_Equalizer_(film)
# Ensure that you print the answers comma separated (As shown in the above line)
# For your convenience, code to read the file q6 is given below. Feel free to modify.
answer_6() {
# Write a function to get the answer to Q6. Do not just echo the answer.
while read line
do
movie=$line
echo "$movie,"
done < q6.
}

# Rank the cities in the file q7 based on their total wikipedia page views for November 2014
# (In descending order of page views, with the highest one first):
# London, Beijing, New_York_City, Bangalore, Tokyo
# Ensure that you print the answers comma separated (As shown in the above line)
# For your convenience, code to read the file q7 is given below. Feel free to modify.
answer_7() {
# Write a function to get the answer to Q7. Do not just echo the answer.
while read line
do
city=$line
echo "$city,"
done < q7
}

# When did the article "Interstellar_(film)" have the most number of page views?
# Input the answer in yyyymmdd format
# Run your commands/code to process the output and echo the answer
# in the above format to standard output
answer_8() {
# Write a function to get the answer to Q8. Do not just echo the answer.
echo 
}

# What is the most popular article of November 2014 with ZERO views on November 1, 2014?
# Run your commands/code to process the output and echo the answer
answer_9() {
# Write a function to get the answer to Q9. Do not just echo the answer.
echo 
}

# Find out the number of articles with longest number of strictly increasing sequence of views
# Example: If 27 articles have strictly increasing pageviews everyday for 5 days (which is the global maximum),
# then your script should find these articles from the output file and return 27.
# Run your commands/code to process the output and echo the answer
answer_10() {
# Write a function to get the answer to Q10. Do not just echo the answer.
echo 
}

# What was the type of the master instance that you used in EMR
# Ungraded question
answer_11() {
# echo the answer (instance type)
echo "m1.large"
}

# What was the type of the task/core instances that you used in EMR
# Ungraded question
answer_12() {
# echo the answer (instance type)
echo "m1.large"
}

# How many task/core instances did you use in your EMR run?
# Ungraded question
answer_13() {
# echo the answer (instance count)
echo "19"
}

# What was the execution time of your EMR run? (You can find this in the EMR console on AWS)
# Please echo just a number (number of minutes)
# Ungraded question
answer_14() {
# echo the answer (execution time in minutes)
echo "65"
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
echo ","

echo -en ' '\"answer9\": \"`answer_9`\"
echo ","

echo -en ' '\"answer10\": \"`answer_10`\"
echo ","

echo -en ' '\"answer11\": \"`answer_11`\"
echo ","

echo -en ' '\"answer12\": \"`answer_12`\"
echo ","

echo -en ' '\"answer13\": \"`answer_13`\"
echo ","

echo -en ' '\"answer14\": \"`answer_14`\"
echo
echo  "}"