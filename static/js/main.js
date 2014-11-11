
var map, pointarray, heatmap;
var minutes = 0;
var now = new Date();
var request_interval = 2000;
var fadeInterval = 800;
var active_heatmap = 0;
var xhr = null;
var requestTimer = null;
var requestTimerSeconds = 0;
var boundsChangedTimer = null;
var auto_step = false;

var heatmapData = [];
google.maps.event.addDomListener(window, 'load', initialize);

function initialize() {
  var mapOptions = {
    zoom: 3,
    mapTypeControl: false,
    streetViewControl: false,
    panControl: false,
    center: new google.maps.LatLng(20.32385600531893, 17.896545250000045),
    mapTypeId: google.maps.MapTypeId.SATELLITE,
    zoomControlOptions: {
        position: google.maps.ControlPosition.LEFT_CENTER
    }
  };

  map = new google.maps.Map(document.getElementById('map-canvas'),
      mapOptions);

  heatmap1 = new google.maps.visualization.HeatmapLayer({});
  heatmap2 = new google.maps.visualization.HeatmapLayer({});

  heatmap1.setMap(map);
  heatmap1.set('radius', 20);

  heatmap2.setMap(map);
  heatmap2.set('radius', 20);

  queryOnZoomChangeSetup();

  requestData(false);
}

function queryOnZoomChangeSetup(){
    google.maps.event.addListener(map, 'bounds_changed', function() {
        runBoundsChangedTimer();
    });
}

function runBoundsChangedTimer(){
  if (boundsChangedTimer != null)
    clearTimeout(boundsChangedTimer);

  boundsChangedTimer = setTimeout(function(){
    requestData(false);
    boundsChangedTimer = null;
  },2000);
}

function fadeinWrapper(heatmap, i){
    window.setTimeout(function(){
        //console.log("fade in at " + 200 * i + " to " + 0.2 * i);
        heatmap.set('opacity', 0.2 * i);
    }, 200 * i);
}

function fadein(heatmap){
    var i = 1;

    for(; i<6; i++){
        fadeinWrapper(heatmap, i);
    }
}


function fadeoutWrapper(heatmap, i){
    window.setTimeout(function(){
        heatmap.set('opacity', i * 0.2);
    }, 200 * i);
}

function fadeout(heatmap){
    for(var i=6; i>1; i--){
        fadeoutWrapper(heatmap, i);       
    } 
}

function setPoints(){
  var pointArray = new google.maps.MVCArray(heatmapData);

  //heatmap1.setData(pointArray);

  if (active_heatmap % 2 == 0){
    heatmap1.set('opacity', 0);
    heatmap1.setData(pointArray);
    
    fadein(heatmap1);
    fadeout(heatmap2);
  }
  else{
    heatmap2.set('opacity', 0);
    heatmap2.setData(pointArray);
    
    fadein(heatmap2);
    fadeout(heatmap1);
  }

  active_heatmap += 1;
}

function get_request_seconds(increment){

  if (increment == true){
    minutes += 60;
    minutes %= 1440;  
  }
  
  var requested_time = new Date(now.getFullYear(), now.getMonth(), now.getDate(), minutes/60, 0, 0);
  var midnight = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);

  return (requested_time.getTime() - midnight.getTime())/1000;
}

function runRequestTimer(){
  requestTimer = setInterval(function(){
    requestTimerSeconds += 1;
    $("#requesttimer").text(requestTimerSeconds + "s");
  }, 1000);
}

function stopRequestTimer(){
  if (requestTimer != null)
    clearInterval(requestTimer);

  $("#requesttimer").text(requestTimerSeconds + "s - done");
  requestTimerSeconds = 0;
  requestTimer = null;
}

function requestData(incrementHour){
  if (xhr != null){
    return;
  }

  console.log("request data");

  var seconds_since_midnight = get_request_seconds(incrementHour);

  var neLat = map.getBounds().getNorthEast().lat();
  var neLon = map.getBounds().getNorthEast().lng();
  var swLat = map.getBounds().getSouthWest().lat();
  var swLon = map.getBounds().getSouthWest().lng();

  $("#bytesprocessed").text("...");
  $("#requesttimer").text("0");

  runRequestTimer();

  xhr = $.ajax({
    type: "GET",
    url: requestUrl,
    timeout: 60000, //60s
    data: { seconds_since_midnight: seconds_since_midnight, neLat: neLat, neLon: neLon, swLat: swLat, swLon: swLon}
  })
  .done(function(response) {
    var responseObj = jQuery.parseJSON(response);
    var newDataSet = responseObj.data;
    
    //console.log(response);
    heatmapData.length = 0;

    for(var i = 0, l = newDataSet.length; i < l; i++){
      var point = {
        location: new google.maps.LatLng(newDataSet[i].lat, newDataSet[i].lon),
        weight: newDataSet[i].count
      }

      heatmapData.push(point);
    }

    setPoints();
    xhr = null;
    stopRequestTimer();

    gbprocessed = parseFloat(responseObj.totalBytesProcessed / 1000000000).toFixed(2);
    ampm = responseObj.seconds_since_midnight/60/60 >= 12 ? "PM" : "AM";
    totalRowsInMillions = responseObj.totalRows;
    cached_hit = responseObj.cached_hit

    $("#hourbox").text(responseObj.seconds_since_midnight/60/60 + ':00' + ampm);

    if (cached_hit == true){
     $("#bytesprocessed").text(gbprocessed + "GB (cached), 1.15 billion rows processed");
    }
    else{
     $("#bytesprocessed").text(gbprocessed + "GB, 1.15 billion rows processed");
    }

    console.log("bytes processed: " + responseObj.totalBytesProcessed);
    console.log("request complete. length is: " + newDataSet.length);

    if (auto_step == true){
      setTimeout(function(){
        requestData(true);
      },2000);
    }
  })
  .fail(function(){
    stopRequestTimer();
    xhr = null;
  });
}

function toggleAutostep(){
  auto_step = !auto_step;

  if (auto_step == true)
    requestData(true);
}

$(document).ready(function(){
    $("#stephourbutton").click(function(){
        requestData(true);
    });    
});
