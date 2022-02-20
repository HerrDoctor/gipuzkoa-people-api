const pg = require('pg');
var express = require('express');
const axios = require('axios');
var fs = require('fs');
const { SNSClient, SubscribeCommand, ConfirmSubscriptionCommand, PublishCommand } = require("@aws-sdk/client-sns");

const basicPOIsUrl = 'http://papi.minube.com/pois?lang=es&country_id=63&zone_id=1010&order_by=score&api_key=V8p7DUAN3G3mwh5H';
const akamaiUrl = 'https://imgs-akamai.mnstatic.com/';

const snsTopic = 'arn:aws:sns:eu-west-1:689618725813:hackaton';

const cities = JSON.parse(fs.readFileSync('./data/cities.json', { encoding: 'utf8' }));
//console.log(JSON.stringify(cities));


exports.handler = async function (event, context, callback) {


    console.log("LAMBDA EVENT: " + JSON.stringify(event));

    let response = {
        statusCode: 500,
        body: null,
        headers: {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": '*',
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
    };
    const client = await openPgConnection();


    try {

        const method = event.httpMethod;
        const resource = event.resource;
        const params = event.queryStringParameters;
        const body = (event.body != null) ? JSON.parse(event.body) : null;
        //const body = event.body;
        console.log("Resource: " + resource + ", Method: " + method + ", params: " + params + ", body: " + body + "Body type" + (typeof body));

        switch (resource) {
            case "/places":


                if (method == "GET") {

                    //const res = await client.query("SELECT * FROM plan");


                    //const city = ("city" in params) ? params.city : null;
                    const city = ("city" in params) ? cities.filter(y => y.city_name == params.city)[0].city_id : null;
                    const category = ("category" in params) ? params.category : null;

                    let urlPOIs = basicPOIsUrl;
                    urlPOIs = ("category" in params) ? (urlPOIs + "&subcategory_id=" + category) : urlPOIs;
                    urlPOIs = ("city" in params) ? (urlPOIs + "&city_id=" + city) : urlPOIs;

                    console.log("URL: " + urlPOIs);

                    const limit = ("limit" in params) ? parseInt(params.limit) : null;

                    console.log("Category: " + category + ", City: " + city + ", Limit: " + limit);
                    
                    let data = [];
                    
                    for (let i = 0; i < 50; i++) {
                        console.log("pre datum");
                        const datum = await axios.get(urlPOIs + '&page=' + i);
                        console.log("pos datum");
                        console.log("Datos obtenidos: " + JSON.stringify(datum.data));
                        data = data.concat(datum.data);
                        if (datum.data.length < 10) {
                            break;
                        }
                        
                        //console.log(datum);
                    }
                    
                    console.log("Data to return: " + JSON.stringify(data));
                    
                    response.body = JSON.stringify(processResult(data, limit));

                    response.statusCode = 200;
                }
            break;
            
            
            case "/email/subscribe":
                
                if (method == "POST") {
                    
                    const snsClient = new SNSClient({ apiVersion: '2010-03-31' });
                    
                    //console.log("SNS client: ");
                    //console.log(snsClient);
                    
                    //console.log("EMAIL: " + JSON.parse(event.body));
                    
                    var emailParams = {
                        Protocol: 'email', /* required */
                        TopicArn: snsTopic, /* required */
                        Endpoint: body.email,
                        ReturnSubscriptionArn: true
                    };
                    
                    let command = new SubscribeCommand(emailParams);
                    const subscription_result = await snsClient.send(command);
                    
                    //const subscription_result = await snsClient.subscribe(emailParams);
                    //console.log("Subscription result: ");
                    //console.log(subscription_result);
                           
                    /*let subscribePromise = snsClient.subscribe(emailParams).promise();

                    // Handle promise's fulfilled/rejected states
                    subscribePromise.then(
                        function(data) {
                            console.log("Subscription ARN is " + data.SubscriptionArn);
                        }).catch(
                        function(err) {
                            console.error(err, err.stack);
                        });*/
                    
                    //const sending_result = await snsClient.publish(snsParams);
                    
                    //await snsClient.publish(snsParams);
                    //console.log("Sending result: ");
                    //console.log(sending_result);
                    
                    response.statusCode = 200;
                    
                }
                
                
            break;


            case "/email/send":
                
                if (method == "POST") {
                    
                    const snsClient = new SNSClient({ apiVersion: '2010-03-31' });
                    
                    //console.log("SNS client: ");
                    //console.log(snsClient);
                    
                    const snsParams = {
                        Message: body.applier + ' se apunta a tu plan. ¡Esto marcha! :)',
                        TopicArn: snsTopic
                    };
                    
                    let command = new PublishCommand(snsParams);
                    const sending_result = await snsClient.send(command);
                    
                    console.log("Sending result: ");
                    console.log(sending_result);
                           
                    /*let subscribePromise = snsClient.subscribe(emailParams).promise();

                    // Handle promise's fulfilled/rejected states
                    subscribePromise.then(
                        function(data) {
                            console.log("Subscription ARN is " + data.SubscriptionArn);
                        }).catch(
                        function(err) {
                            console.error(err, err.stack);
                        });*/
                    

                    //const sending_result = await snsClient.publish(snsParams);
                    
                    //await snsClient.publish(snsParams);
                    //console.log("Sending result: ");
                    //console.log(sending_result);
                    
                    response.statusCode = 200;
                    
                }
                
                
            break;
            


        default:
            console.log("WRONG ENDPOINT");
        break;
        }

        //response.body = JSON.stringify(res.rows);

    } catch (err) {

        throw err;
        //error message
    }
    finally {

        client.end();
        console.log("FINAL RESPONSE: " + JSON.stringify(response));
        return response;

    }

};



async function openPgConnection() {
    const client = new pg.Client({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_DATABASE,
        password: process.env.DB_PASSWORD,
        port: 5432
    });
    await client.connect();
    return client;
}

async function dummy() {
    return 5;
}


function processResult(result, limit) {
    
    console.log("Processing result...");
    
    if (limit != null && result.length >= limit) {
        result.splice(limit);
    }
    
    console.log("Sin procesar... " + JSON.stringify(result));
    
    const result2 = result.map(x => {
        x.city_name = cities.filter(y => y.city_id == x.city_id)[0].city_name;
        delete x.country_id;
        delete x.zone_id;
        if ("picture_id" in x) {
            x.picture_url = fix_picture_url(x.picture_url);
        }
        return x;
    });
    
    console.log("Procesado: " + JSON.stringify(result2));
    
    return result2;
}

function fix_picture_url(url) {
    const hash_parts = url.split('/');
    const hash = hash_parts[hash_parts.length-1];
    return (akamaiUrl + hash.substring(0, 2) + "/" + hash.substring(2, 4) + "/" + hash + ".jpg");
}