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

echo '--------------------------------'
echo "Create Index on table1"
java createindex table1 2 2 3

echo '--------------------------------'
echo "Performing Filter Query"
java query2 table1 table2 ./datafiles/phase3/custom_data/fquery1.txt 1000