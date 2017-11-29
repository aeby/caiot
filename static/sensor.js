(function () {
    var ws_scheme = window.location.protocol == "https:" ? "wss" : "ws";
    var socket = new ReconnectingWebSocket(ws_scheme + '://' + window.location.host);
    var logger = document.getElementById('logger'),
        status = document.getElementById('status'),
        temp = document.getElementById('temp');

    socket.onmessage = function (message) {
        var data = JSON.parse(message.data);
        logger.value = message.data + '\n' + logger.value;
        if (data.deviceParameter === 'Temperature') {
            temp.textContent = 'Temperature ' + data.deviceValue + '°C';
            if (data.deviceValue > 25) {
                status.classList.remove('status-green');
                status.classList.add('status-red');
            } else {
                status.classList.remove('status-red');
                status.classList.add('status-green');
            }
        }
    };
})();