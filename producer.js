var requestClient = require('request-promise');
var kafka = require('kafka-node');

let Producer = kafka.Producer;
let KeyedMessage = kafka.KeyedMessage;
let client = new kafka.KafkaClient();
let producer = new Producer(client);

let countriesTopic = "countries";
let countryProducerReady = false;
let countries = null;

producer.on('ready', function () {
    console.log("Producer for countries is ready");
    countryProducerReady = true;
});

producer.on('error', function (err) {
    console.error("Problem with producing Kafka message " + err);
})

let averageDelay = 3000;  // in miliseconds
let spreadInDelay = 2000; // in miliseconds
var delay = averageDelay + (Math.random() - 0.5) * spreadInDelay;

init();

async function init() {
    countries = await getCountriesData();
    countries = JSON.parse(countries);
    handleCountry(1);
}

function handleCountry(currentCountry) {
    var line = countries[currentCountry];
    if (line) {
        var country = {
            "name": line["name"]
            , "region": line["region"]
            , "subregion": line["subregion"]
            , "population": line["population"]
            , "currencies": line["currencies"]
        };

        console.log('Country which needs to be send by ProducerContry : ', JSON.stringify(country));

        produceCountryMessage(country);

        setTimeout(handleCountry.bind(null, currentCountry + 1), delay);
    }
};

function produceCountryMessage(country) {

    let keyedMsg = new KeyedMessage(country.name, JSON.stringify(country));
    let payloads = [
        { topic: countriesTopic, messages: keyedMsg, partition: 0 },
    ];

    if (countryProducerReady) {
        producer.send(payloads, function (err, data) {
            console.log('CountryProducer sent data: ', JSON.stringify(data));
        });
    } else {
        console.error("sorry, CountryProducer is not ready yet, failed to produce message to Kafka.");
    }
}

async function getCountriesData() {
    var options = {
        uri: 'https://restcountries.eu/rest/v2/all',
        method: 'GET'
    };

    return res = await requestClient(options);
}




