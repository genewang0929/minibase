echo '--------------------------------'
echo "Go to src directory"
cd ../../../

echo '--------------------------------'
echo "Compiling Java files"
make db
javac -encoding UTF-8 batchcreate.java createindex.java batchinsert2.java batchdelete.java query2.java

echo '--------------------------------'
echo "Remove old database instance"
rm ./dbinstance/mydb

echo '--------------------------------'
echo "Creating database mydb, table1"
java batchcreate ./datafiles/phase3/custom_data/create_for_delete_1.txt table1