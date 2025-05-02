echo '--------------------------------'
echo "Go to src directory"
cd ../../

echo '--------------------------------'
echo "Compiling Java files"
make db
javac -encoding UTF-8 batchcreate.java createindex.java batchinsert2.java batchdelete.java query2.java

echo '--------------------------------'
echo "Remove old database instance"
rm ./dbinstance/mydb

echo '--------------------------------'
echo "Creating database mydb, table1"
java batchcreate ./datafiles/phase3/data_1.txt table1

echo '--------------------------------'
echo "Create Index on table1"
java createindex table1 2 2 3

echo '--------------------------------'
echo "Delete records on table1"
java batchdelete ./datafiles/phase3/delete_1.txt table1