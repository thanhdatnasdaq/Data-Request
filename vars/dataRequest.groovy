def call(projectName, requiredAgent='java8'){
def exeQueue = [], failList = []
pipeline{
    agent {
        label "apac_datarequest"
    }
    // :storage/broker
    parameters{
        choice(choices: ["echo", "sh"], description: "What command to execute?", name: "executeCommand")
        booleanParam(defaultValue: true, description: "Confirm to deploy?", name: "confirmDeploy")
    }
    
    stages{
        stage("Set Up"){
            steps{
                    echo "seting up ..."
                    echo "Java Home: $JAVA_HOME"
                    sh 'java -version'
                }
            }
        stage("Build"){
            steps{
                    echo "building ..."
                    sh "whoami"
                    sh "pwd"
                    sh "env"
                    echo "Running ${env.BUILD_ID} on ${env.JENKINS_URL}"
                    script{                        
                        def map = mapping()
                        def subscription = [], startDate = [], endDate = [], currentServer = []
                        readDataRequest(subscription, startDate, endDate)
                        for(int i = 0; i < subscription.size; i++){
                            def includeCommand = generateIncludeCommand(startDate[i], endDate[i])
                            def serverCommand = findTargetServer(map, subscription[i], startDate[i], endDate[i], failList, currentServer)
                            if(!serverCommand.equals("Not Found")){
                                execute(includeCommand, serverCommand, exeQueue, currentServer[currentServer.size - 1], subscription[i])
                            }
                        }
                    }
                    
                }
            }
        stage("Processing"){
            steps{
                script{
                    parallel(
                        thread1:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        },
                        thread2:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                        thread3:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                        thread4:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                        thread5:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                        thread6:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                        thread7:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                        thread8:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                        thread9:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                        thread10:{
                            if(!exeQueue.isEmpty()){ executeParallel(exeQueue)}
                        }
                    )
                    if(!exeQueue.isEmpty()){ executeHelper(exeQueue)}
                    if (failList.size > 0){
                        println("Unable to find target servers for these: \n${failList}")
                    } 
                }
            }
        }
        stage("Deploy"){
            steps{
                echo "deploying ..."
                echo "confirmDeploy: ${params.confirmDeploy}"
                script{
                    if(params.confirmDeploy == false){
                        echo "Fail to deploy"
                        }else{
                        currentBuild.result = "SUCCESS"
                        echo "Status: ${currentBuild.result}"
                        }
                    }
                }
            }
        }
    }
}

/**
* This function is used to execute the command from user
*/
def execute(String includeCommand, String serverCommand, List exeQueue,  String currentServer, String subscription){
    if(!currentServer.substring(0,4).equals("apse")){
        starterCommand = "rsync --progress -a --stats --timeout=900 -rKLOmtzv --no-h --exclude=*.fav --exclude=*.watchlist.txt --exclude=*.raw --exclude=*.removed --exclude=*ControlTimes.cfg "
    }else{
        starterCommand = "rsync --progress -e \"ssh -l datarequest -i /var/lib/favsync/.ssh/id_rsa_datarequest -o StrictHostKeyChecking=no\" --include=${subscription}/ "
    }
    if(params.executeCommand.equals("echo")){
        echo "${starterCommand} $includeCommand $serverCommand  /data01/source/data/$subscription/"
    }else if(params.executeCommand.equals("sh")){
        List temp = []
        temp.add("${starterCommand} $includeCommand $serverCommand")
        temp.add(currentServer)
        temp.add(subscription)
        exeQueue.add(temp)
    }else{
        echo "Invalid command"
    }
}

/**
* This function is used to execute each rsync command and set limit for each host at 5 maximum execution at once
*/
def executeHelper(List exeQueue){
    println("Total processing subscription match database: ${exeQueue.size}")
    while(exeQueue.size > 0){
        Integer progress = """${sh(returnStdout: true, script: "pgrep ${exeQueue[0][1]} | wc -l") }""" as Integer
        if(progress < 5){
            def detail = []
            detail = exeQueue[0][2].split('_')
            echo "-----------------------------------------------------------------"
            echo "current progress for run on ${exeQueue[0][1]}: ${progress}"
            echo "Broker: ${detail[0]}"
            echo "Market: ${detail[1]}"
            if(detail[0].equals("jpmorgan")){
                
                sh """
                    cd  /data01/source/data/${exeQueue[0][2]}/
                    tar -cf track.tar track
                """
                
                sh "${exeQueue[0][0]} /data01/source/temp/${exeQueue[0][2]}/"
                sh """
                    export JAVA_HOME="/usr/lib/jvm/jre-1.8.0-openjdk-1.8.0.332.b09-1.el7_9.x86_64"
                    cd  /data01/source/temp/${exeQueue[0][2]}/track
                    obfuscator-crawler -b jpmorgan /etc/datarequest-csi/obfuscation.xml . /data01/source/data/${exeQueue[0][2]}/track
                """
            }else{

                sh "${exeQueue[0][0]} /data01/source/data/${exeQueue[0][2]}/track"
            }
            echo "-----------------------------------------------------------------"
       }else{
        exeQueue.add(execute[0])
       }
        exeQueue.remove(0)
    }
}

def executeParallel(List exeQueue){
    //println("Total processing subscription match database: ${exeQueue.size}")
    while(exeQueue.size > 10){
        Integer progress = """${sh(returnStdout: true, script: "pgrep ${exeQueue[0][1]} | wc -l") }""" as Integer
        List queue = exeQueue[0]  
        exeQueue.remove(0)
        if(progress < 5){
            def detail = []
            detail = queue[2].split('_')
            echo "-----------------------------------------------------------------"
            echo "current progress for run on ${queue[1]}: ${progress}"
            echo "Broker: ${detail[0]}"
            echo "Market: ${detail[1]}"
            if(detail[0].equals("jpmorgan")){
                
                sh """
                    cd  /data01/source/data/${queue[2]}/
                    tar -cf track.tar track
                """
                
                sh "${queue[0]} /data01/source/temp/${queue[2]}/"
                sh """
                    export JAVA_HOME="/usr/lib/jvm/jre-1.8.0-openjdk-1.8.0.332.b09-1.el7_9.x86_64"
                    cd  /data01/source/temp/${queue[2]}/track
                    obfuscator-crawler -b jpmorgan /etc/datarequest-csi/obfuscation.xml . /data01/source/data/${queue[2]}/track
                """
            }else{

                sh "${queue[0]} /data01/source/data/${queue[2]}/track"
            }
            echo "-----------------------------------------------------------------"
       }else{
        exeQueue.add(queue)
       }
    }
}

/**
* This function is used to generate the include commands between the start date and end date
*/
def generateIncludeCommand(String startDate, String endDate){
    //The result of the command will be generated from start date to end date user input
    String command = ""
    //parse the start to end date from string to date datatype, creates other empty list to store all day between startto end date
    def start = Date.parse("yyyyMMdd", startDate)
    def end = Date.parse("yyyyMMdd", endDate)
    def allDaysList = []
    def allMonthList = []
    def allYearList = []
    while(start <= end){
        allDaysList.add(start.format('yyyyMMdd'))
        start = start.plus(1)
        if(allMonthList.isEmpty()){
            allMonthList.add(start.format('yyyyMMdd').substring(0,6))
        }else if(allMonthList[allMonthList.size() - 1] != start.format('yyyyMMdd').substring(0,6)){
            allMonthList.add(start.format('yyyyMMdd').substring(0,6))
        }
        if(allYearList.isEmpty()){
            allYearList.add(start.format('yyyyMMdd').substring(0,4))
        }else if(allYearList[allYearList.size() - 1] != start.format('yyyyMMdd').substring(0,4)){
            allYearList.add(start.format('yyyyMMdd').substring(0,4))
        }
    }
    command += " --include=track/ --include=daytot/ --include=missing_dates.txt --include=*.name --include=*.dat --include=*.cfg --include=houses.xml"
    for(year in allYearList){ 
        command += " --include=track/${year}/ --include=daytot/${year}/" 
    }

    for(timeline in allMonthList){
        String year = timeline.substring(0,4)
        String month = timeline.substring(4)

        //This line will generate --include=track/yyyy/mm/
        command += " --include=track/${year}/${month}/"
        //This line will generate --include=daytot/yyyy/mm/
        command += " --include=daytot/${year}/${month}/"
    }

    for(timeline in allDaysList){
        String year = timeline.substring(0,4)
        String month = timeline.substring(4,6)
        //This line will generate --include=track/yyyy/mm/yyyymmdd*
        command += " --include=track/${year}/${month}/${timeline}*"
    }

    for(timeline in allDaysList){
        String year = timeline.substring(0,4)
        String month = timeline.substring(4,6)
        //This line will generate  --include=daytot/yyyy/mm/yyyymmdd/
        command += " --include=daytot/${year}/${month}/${timeline}/"
        //This line will generate --include=daytot/yyyy/mm/yyyymmdd/yyyymmdd*
        command += " --include=daytot/${year}/${month}/${timeline}/${timeline}*"
    }
    return command
}

/**
* This function is used to map the all brokers with appropriate subscriptions and target servers

Ex: 
@Input: public_iceeu,boci-emea,nl01smbcalwebp57.int.smbc.nasdaqomx.com
        fiemea_20151001_20180126,nomura-emea,axon.int.smbc.nasdaqomx.com
@Output: 
        map = {   
            public_iceeu:[boci-emea, nl01smbcalwebp57.int.smbc.nasdaqomx.com], 
            fiemea_20151001_20180126:[nomura-emea, axon.int.smbc.nasdaqomx.com]
        }
*/
def mapping(){
    def list = readFile("hostMap.csv").readLines()
    def map = [:]
    for(line in list){
        def data = [], temp = []
        data = line.split(',')
        if(!map.containsKey(data[0])){
            temp.add(data[1])
            temp.add(data[2])
            map.put(data[0], [temp])
        }else{
            temp = map.get(data[0])
            temp.add([data[1], data[2]])
            map.put(data[0], temp)
        }
    }
    return map
}

/**
* This function is used to find the target server for each subscription
by checking with the data store in map
Ex: if subscription: cimb_ath_m then subscription details = [cimb, ath, m]
=> Broker: cimb, Market : ath  ==> Target Sever : aldo.int.smbc.nasdaqomx.com
*/
def findTargetServer(def map, def subscription, def startDate, def endDate, def failList, def currentServer){
    def subscriptionDetails = [], targetServer = ""
    subscriptionDetails = subscription.split('_')
    if(!map.containsKey(subscriptionDetails[1])){
        failList.add(subscription)
        return "Not Found"
    }else{
        for(item in map.get(subscriptionDetails[1])){
            if (item[0].contains(subscriptionDetails[0])){
                targetServer = item[1]
            }
        }
    }
    if(targetServer.equals("")){
        failList.add(subscription)
        return "Not Found"
    }
    currentServer.add(targetServer)
    return " --exclude=* $targetServer::data-requests/${subscription}/"
}

/**
This function is used to read all the data input received from file that user uploaded 
and store in parralel lists according to their deitals of subscription, start and end dates 
of transaction
*/
def readDataRequest(def subscription, def startDate, def endDate){
    def data = readFile("sample.txt").readLines()
    for(line in data){
        def details = line.split(', ')
        subscription.add(details[0])
        startDate.add(details[1])
        endDate.add(details[2])
    }
}