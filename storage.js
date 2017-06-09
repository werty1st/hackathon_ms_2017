const azure = require('azure-storage');
const blobService = azure.createBlobService();
const request = require('request');


const containername = process.env.CONTAINERNAME;
const SUBSCRIPTIONKEY = process.env.SUBSCRIPTIONKEY;

const EXZTAUTHU = process.env.EXZTAUTHU;
const EXZTAUTHP = process.env.EXZTAUTHP;

let tasksopen = [];
let tasksbreakdown = [];

let mappingtable = {};

const levelup = require('levelup');
const db = levelup('./mydb');

//const db = JSON.parse(fs.readFileSync('database'));

function getVideoIdMapping(){

    console.log("Get getVideoIdMapping DB");

    request({
        method: 'GET',
        auth: {
            'user': EXZTAUTHU,
            'pass': EXZTAUTHP,
            'sendImmediately': false
        },
        url: `http://video-hackathon.test7.exozet.com/api/videos`,
        headers: {
        }
    }, function (error, response, body) {

        if (!error && response.statusCode == 200) {
            const results = JSON.parse(body);          
            results.every((item)=>{
                mappingtable[item.fileName] = item.id;
                return true;
            });            

        } else {
            console.error("Error",error,body);
        }
    });    
}
getVideoIdMapping();


blobService.listBlobsSegmented(containername , null, function(error, result, response){
    if(!error){
        // result.entries contains the entries
        // If not all blobs were returned, result.continuationToken has the continuation token.

        result.entries.every((BlobResult)=>{

            if (BlobResult.name.endsWith(".mp4") ){

                //check if videos is processed already
                let exozetID = mappingtable[BlobResult.name]

                db.get(exozetID,(err,value)=>{
                    if (value){
                        console.log("ignore video",BlobResult.name);
                    } else {
                        tasksopen.push(BlobResult);                        
                    }
                })
            }
            return true;
        });

        startVideoWorker();

    }
});

function startVideoWorker(){
    if (tasksopen.length == 0){
        console.log("no videos left");
        return;
    }
    let video = tasksopen.pop();
    processVideo(video);
    setTimeout(startVideoWorker,10000);
}

function processVideo(video){
    //triger videoindex with video

    let uri = blobService.getUrl(containername, video.name);
    console.log("Analyze Video:", video.name);

    const apiUrl = "https://videobreakdown.azure-api.net/Breakdowns/Api/Partner/Breakdowns";

    request({
        method: 'POST',
        url: `${apiUrl}?name=${video.name}&privacy=private&videoUrl=${uri}&language=german`,
        headers: {
            'Content-Type': 'multipart/form-data',
            'Ocp-Apim-Subscription-Key': SUBSCRIPTIONKEY
        }
    }, function (error, response, body) {

        if (!error && response.statusCode == 200) {
            var taskId = JSON.parse(body);
            
            console.log("task id", taskId);

        } else {
            console.error("Error1",error,body);
        }
    });    
}

let timer1 = setInterval(searchVideoTask,5000);
// function watchVideoTask(){

//     // let id = tasksopen.pop();

//     // if (!id)return;
//     id = "420df9e2a2";
//     console.log("Query Task State for id", id);

//     request({
//         method: 'GET',
//         url: `https://videobreakdown.azure-api.net/Breakdowns/Api/Partner/Breakdowns/${id}/State`,
//         headers: {
//             'Ocp-Apim-Subscription-Key': SUBSCRIPTIONKEY
//         }
//     }, function (error, response, body) {

//         if (!error && response.statusCode == 200) {
            
//             console.log("task state", body);

//         } else {
//             console.error("Error",error,body);
//         }
//     });
// }

function searchVideoTask(){

    console.log("Query Task State for all videos");

    request({
        method: 'GET',
        url: `https://videobreakdown.azure-api.net/Breakdowns/Api/Partner/Breakdowns/Search`,
        headers: {
            'Ocp-Apim-Subscription-Key': SUBSCRIPTIONKEY
        }
    }, function (error, response, body) {

        if (!error && response.statusCode == 200) {
            const result = JSON.parse(body);          

            let counter = result.results.length;
            let arrResult = result.results.every((video=>{
                //console.log("video:", video.name, "state", video.state, "processingProgress", video.processingProgress);
                if (video.state == "Processed"){
                    tasksbreakdown.push(video.id);
                    counter -= 1;
                } else {
                    console.log("ask again later", video.id);
                }
                return true;
            }));
            getBreakdown();

            if (arrResult && counter == 0){
                console.log("stop timer all videos done");
                clearInterval(timer1);
            }

        } else {
            console.error("Error2",error,body);
        }
    });
}

searchVideoTask();


function getBreakdown(){


    if (tasksbreakdown.length == 0){
        console.log("no tasksbreakdown left");
        return;
    }

    let id = tasksbreakdown.pop();

    console.log("Get Result for id", id);

    request({
        method: 'GET',
        url: `https://videobreakdown.azure-api.net/Breakdowns/Api/Partner/Breakdowns/${id}`,
        headers: {
            'Ocp-Apim-Subscription-Key': SUBSCRIPTIONKEY
        }
    }, function (error, response, body) {

        if (!error && response.statusCode == 200) {
            const results = JSON.parse(body);          
            pushBrakedown(results.summarizedInsights);

        } else {
            console.error("Error5",error,body);
        }
        setTimeout(getBreakdown,5000);
    });    
}


function pushBrakedown(summarizedInsights){
        
   let exozetID = mappingtable[summarizedInsights.name];

   db.get(exozetID, (err,value)=>{
       if (value){
           console.log("Skip video id:", exozetID);
       } else {

            console.log("Mapping",summarizedInsights.name, "=", exozetID);

            request({
                method: 'PATCH',
                auth: {
                    'user': EXZTAUTHU,
                    'pass': EXZTAUTHP,
                    'sendImmediately': false
                },
                url: `http://video-hackathon.test7.exozet.com/api/videos/${exozetID}`,
                headers: {
                    'content-type': "application/x-www-form-urlencoded"
                },
                form: { "rawVideoIndexerOutput": JSON.stringify(summarizedInsights) }
            }, function (error, response, body) {

                const id = exozetID;
                if (!error && response.statusCode == 200) {
                    let result = JSON.parse(body);
                    if (result.message == `updated video #${id}`){
                        db.put(id, true, (err, value)=>{
                            if (!err){
                                console.log("Video analysis completed");
                            } else {
                                console.log("Error4 with video",id);
                            }
                        })
                    }
                } else {
                    console.error("Error3",error,body);
                }
            });  
       }
   })

  

}


