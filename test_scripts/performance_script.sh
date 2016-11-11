echo "-----------------------DATABASE FILE SYSTEM TEST  -----------------------------"
mkdir_test(){
echo "***TESTING  MKDIR***"
root=./fusemount/
   
     mkdir ./fusemount/dir0 
     mkdir ./fusemount/dir1 
     mkdir ./fusemount/dir2 
     mkdir ./fusemount/dir3 
     mkdir ./fusemount/dir4 

echo "***TESTING  FILE SYSTEM DEPTH***"
path_name="./fusemount/dir0/"   
   for ((i = 0; i < 2; i++))
   do
     file_name="dir0_$i/"
     file_name=$path_name$file_name
     mkdir $file_name 
     path_name=$file_name 
   done
}



list_files(){
for ((i=0; i < 1000; i++))
do
  ls  fusemount 
done
}


echo "--------Writing to Data Base/cache Time=mkdir--------"   
mkdir_test 

echo "-----Reading from Data Base/cache Time= ls  --------"  
list_files  

