var N3 = require('n3'),
    request = require('request'),
	urlparse = require('url-parse');

var SAMPLING_FACTOR = 0.5;

/**
 * Gets the details about the biggest datasets that are available on the laundromat.
 * @returns {Promise}
 */
function getRelevantDatasets() {
    var laundromatURL = "http://lodlaundromat.org/sparql/";
    //TODO remove max triples filter, adjust limit
    var query = '\
    PREFIX llo: <http://lodlaundromat.org/ontology/>\
    PREFIX ll: <http://lodlaundromat.org/resource/>\
    SELECT ?md5 ?doc ?triples {\
      [] llo:triples ?triples;\
        llo:url ?doc;\
        llo:md5 ?md5.\
    FILTER(?triples > 0)\
    FILTER(?triples < 1000)\
    } ORDER BY DESC(?triples)\
    LIMIT 10';

    var requestParams = {
        url: laundromatURL,
        form: {query: query},
        headers: {Accept: 'application/json'}
    };
    return new Promise(function(resolve, reject) {
        request.post(requestParams, function (err, resp, body) {
            if (err) {
                reject(err);
            } else if (resp.statusCode !== 200) {
                reject(new Error('Statuscode ' + resp.statusCode + ' while retrieving datasets.'));
            } else {
                body = JSON.parse(body);
                resolve(body.results.bindings.map(function(elem) {
                    return {
                        ldfEndpoint: 'http://ldf.lodlaundromat.org/' + elem.md5.value,
                        source: elem.doc.value,
                        triples: elem.triples.value
                    };
                }));
            }
        });
    });
}

/**
 * Requests the page with the specified url and performs the callback on each triple on that page.
 * @param url
 * @param callback will be called with argument null when all triples are processed.
 */
function streamTriples(url, callback) {
    var requestOptions = {
        url: url,
        headers: {Accept: 'application/trig'}
    };

    request(requestOptions, function (err, response, body) {
        if (err) {
            callback(err);
            return;
        }
        if (response.statusCode != 200) {
            callback(new Error("Encountered statuscode " + response.statusCode + " for " + ldfURL));
            return;
        }
        N3.Parser({format: 'application/trig'}).parse(body, function (err, triple, prefix) {
            if (err) {
                callback(err);
                return;
            }
            //triple is null when parsing has been completed
            callback(null, triple);
        });
    });
}



function analyseData(ldfURL) {
    return new Promise(function(resolve, reject){
        streamTriples(ldfURL, function(err, triple) {
            if (err) {
                reject(err);
                return;
            }
            //Get the number of triples in the dataset
            if (triple && triple.subject === ldfURL && triple.predicate === 'http://rdfs.org/ns/void#triples') {
                var numTriples = parseInt(N3.Util.getLiteralValue(triple.object));
                var maxPage = Math.ceil(numTriples / 100.);
                var pagesToSample = Math.ceil(numTriples * SAMPLING_FACTOR / 100);
                var pageInterval = maxPage / pagesToSample;

                var remainingPages = pagesToSample;
                var urls = {};

                //Sample uniformly distributed pages of the datasets so we (hopefully) get a representative distribution
                // of the resources being used.
                for (var i = 0; i < pagesToSample; i++) {
                    var pageURL = ldfURL + "?page=" + (1 + i*pageInterval);
                    //console.log(pageURL);
                    streamTriples(pageURL, function(err, triple) {
                        if (err) {
                            reject(err);
                            return;
                        }
                        if (triple) {
                            //Skip LDF metadata
                            if (triple.graph !== '') {
                                return;
                            }
                            [triple.subject, triple.predicate, triple.object].forEach(function(elem) {
                                if (N3.Util.isIRI(elem)) {
                                    var host = urlparse(elem).host;
                                    urls[host] = (urls[host] || 0) + 1;
                                }
                            });
                        } else {
                            remainingPages = remainingPages - 1;
                            if (remainingPages == 0) {
                                resolve(urls);
                            }
                        }
                    })
                }
            }
        })
    });
}

getRelevantDatasets().then(function(datasets){
    return Promise.all(datasets.map(function(dataset) {
        return analyseData(dataset.ldfEndpoint).then(function(analysis) {
            dataset.hostOccurrences = analysis;
            return dataset;
        })
    }))
}).then(function(analysis) {
    console.log(analysis);
    console.log("all done");
}).catch(function(err) {
    console.log("An error occurred: " + err);
});

