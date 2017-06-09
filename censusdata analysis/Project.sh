#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************APP MENU***********************${NORMAL}"
    echo -e 
    echo -e "${MENU}****${NUMBER} 1)${MENU} Connect To Hadoop ${NORMAL}"
    echo -e
    echo -e "${MENU}****${NUMBER} 2)${MENU} Import Data In HIVE ${NORMAL}"
    echo -e
    echo -e "${MENU}****${NUMBER} 3)${MENU} Perform Queries ${NORMAL}"
    echo -e
    echo -e "${MENU}*****************************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}

function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
        option_picked "Connect To HADOOP And Load Data Into HDFS";
	hadoop fs -rm -r /Project;	
     	hadoop fs -put Project /Project;
        echo -e "Data is loaded into HDFS";
        show_menu;
        ;;

       
	2) clear;
        option_picked "Import Data into HIVE";
        hive -f hdfs://localhost:54310/Project/HiveQuery/hivequery.hql
        echo -e "Data is loaded into Hive Table";
        show_menu;
        ;;
         
   
        3) clear;
        option_picked "Select the query you want to perform";
	echo -e
        echo -e "${MENU}****${NUMBER} 1)${MENU}Ratio of Male And Female population to the total population.(MAPREDUCE)${NORMAL}"
	echo -e "${MENU}****${NUMBER} 2)${MENU}List of Male and Female citizens who are senior citizen and are still working(MAPREDUCE)${NORMAL}"
	echo -e "${MENU}****${NUMBER} 3)${MENU}Total Tax Paid by working adults (to be considered in various age groups)(MAPREDUCE)${NORMAL}"
	echo -e "${MENU}****${NUMBER} 4)${MENU}Average income of persons whose age>25 and are not citizens of US.(HIVE)${NORMAL}"
	echo -e "${MENU}****${NUMBER} 5)${MENU}Ratio of married to unmarried or single people (excluding children)(HIVE)${NORMAL}"
	echo -e "${MENU}****${NUMBER} 6)${MENU}People paying tax more than the average tax of all citizens working(HIVE)${NORMAL}"
	echo -e "${MENU}****${NUMBER} 7)${MENU}Ratio of Number of widowed females working to those not working.(PIG)${NORMAL}"
	echo -e "${MENU}****${NUMBER} 8)${MENU}Persons who are working, but not filing tax.(PIG)${NORMAL}"
	echo -e "${MENU}****${NUMBER} 9)${MENU}People paying tax less than the average tax of all citizens working(PIG)${NORMAL}"
        echo -e
        echo -e "${MENU}**************************************************************************************************************************${NORMAL}"
	read n
	    case $n in

                    1)	echo "Ratio of Male And Female population to the total population.(MAPREDUCE)"
                    hadoop fs -rm -r /Project/Output/MapReduce/MTF_Ratio;
		    hadoop jar Project/project.jar MTF_Ratio /Project/Data/census_voter/000000_0 /Project/Output/MapReduce/MTF_Ratio;
                    hadoop fs -cat /Project/Output/MapReduce/MTF_Ratio/part-r-00000;
                   ;;			
                    


                    2) 	echo "List of Male and Female citizens who are senior citizen and are still working(MAPREDUCE)"
                    hadoop fs -rm -r /Project/Output/MapReduce/MTF_Scz_List;
		    hadoop jar Project/project.jar MTF_Scz_List /Project/Data/census_voter/000000_0 /Project/Output/MapReduce/MTF_Scz_List;
                    hadoop fs -cat /Project/Output/MapReduce/MTF_Scz_List/part-m-00000;
                    ;;
                    

                    3) 	echo "Total Tax Paid by working adults (to be considered in various age groups)(MAPREDUCE)"
                    hadoop fs -rm -r /Project/Output/MapReduce/TaxPaid;
		    hadoop jar Project/project.jar TaxPaid /Project/Data/census_voter/000000_0 /Project/Output/MapReduce/TaxPaid;
                    hadoop fs -cat /Project/Output/MapReduce/TaxPaid/part-r-00000;
                    ;;
                    

                    4) 	echo "Average income of persons whose age>25 and are not citizens of US.(HIVE)"
                    hive -f hdfs://localhost:54310/Project/HiveQuery/hive1.hql
                    ;;

                    
           	    5) 	echo "Ratio of married to unmarried or single people (excluding children)(HIVE)"
                    hive -f hdfs://localhost:54310/Project/HiveQuery/hive2.hql
                    ;;

		    6) 	echo "People paying tax more than the average tax of all citizens working(HIVE)"
                    hive -f hdfs://localhost:54310/Project/HiveQuery/hive3.hql
                    ;;

 		    7) 	echo "Ratio of Number of widowed females working to those not working.(PIG)"
                    pig -x mapreduce -f hdfs://localhost:54310/Project/PigQuery/pigquery7.pig
                    ;;

 		    8) 	echo "Persons who are working, but not filing tax.(PIG)"
                    pig -x mapreduce -f hdfs://localhost:54310/Project/PigQuery/pigquery8.pig
                    ;;

		    9) 	echo "People paying tax less than the average tax of all citizens working(PIG)"
                    pig -f hdfs://localhost:54310/Project/PigQuery/pigquery9.pig
                    ;;

                    
                *) echo "Please Select one among the option[1-9]";;
                esac
                show_menu;
             ;;
		
     
            
   \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done


