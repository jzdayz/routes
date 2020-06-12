/*-
 * #%L
 * monitor
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2019 jzdayz
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
@file:JvmName("SseApplication")

package io.github.jzdayz.server
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.CacheControl
import io.ktor.http.ContentType
import io.ktor.http.content.static
import io.ktor.response.cacheControl
import io.ktor.response.respondText
import io.ktor.response.respondTextWriter
import io.ktor.routing.Route
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.broadcast
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay

/**
 * SSE (Server-Sent Events) sample application.
 * This is the org.route.server.main entrypoint of the application.
 */
@UseExperimental(ExperimentalCoroutinesApi::class)
fun <T> doReportServer(report : T,port : Int) {
    /**
     * Here we create and start a Netty embedded server listening to the port 8080
     * and define the org.route.server.main application module inside the specified lambda.
     */


    embeddedServer(Netty, port = port) { // this: Application ->

        /**
         * We produce a [BroadcastChannel] from a suspending function
         * that send a [SseEvent] instance each second.
         */
        val channel = produce { // this: ProducerScope<org.route.server.SseEvent> ->
            var n = 0
            while (true) {
                send(SseEvent(report.toString()))
                delay(1000)
                n++
            }
        }.broadcast()

        /**
         * We use the [Routing] feature to declare [Route] that will be
         * executed per call
         */
        routing {
            /**
             * Route to be executed when the client perform a GET `/sse` request.
             * It will respond using the [respondSse] extension method defined in this same file
             * that uses the [BroadcastChannel] channel we created earlier to emit those events.
             */
            get("/sse") {
                val events = channel.openSubscription()
                try {
                    call.respondSse(events)
                } finally {
                    events.cancel()
                }
            }
            /**
             * Route to be executed when the client perform a GET `/` request.
             * It will serve a HTML file embedded directly in this string that
             * contains JavaScript code to connect to the `/sse` endpoint using
             * the EventSource JavaScript class ( https://html.spec.whatwg.org/multipage/comms.html#the-eventsource-interface ).
             * Normally you would serve HTML and JS files using the [static] method.
             * But for illustrative reasons we are embedding this here.
             */
            get("/") {
                call.respondText(
                        """
                        <html>
                            <head></head>
                            <body>
                                <ul id="events">
                                </ul>
                                <script type="text/javascript">
                                    var source = new EventSource('/sse');
                                    var eventsUl = document.getElementById('events');

                                    function logEvent(text) {
                                        var li = document.createElement('li')
                                        li.innerText = text;
                                        eventsUl.appendChild(li);
                                    }

                                    source.addEventListener('message', function(e) {
                                        logEvent('message:' + e.data);
                                    }, false);

                                    source.addEventListener('open', function(e) {
                                        logEvent('open');
                                    }, false);

                                    source.addEventListener('error', function(e) {
                                        if (e.readyState == EventSource.CLOSED) {
                                            logEvent('closed');
                                        } else {
                                            logEvent('error');
                                            console.log(e);
                                        }
                                    }, false);
                                </script>
                            </body>
                        </html>
                    """.trimIndent(),
                        contentType = ContentType.Text.Html
                )
            }
        }
    }.start(wait = true)
}

/**
 * The data class representing a SSE Event that will be sent to the client.
 */
data class SseEvent(val data: String, val event: String? = null, val id: String? = null)

/**
 * Method that responds an [ApplicationCall] by reading all the [SseEvent]s from the specified [events] [ReceiveChannel]
 * and serializing them in a way that is compatible with the Server-Sent Events specification.
 *
 * You can read more about it here: https://www.html5rocks.com/en/tutorials/eventsource/basics/
 */
suspend fun ApplicationCall.respondSse(events: ReceiveChannel<SseEvent>) {
    response.cacheControl(CacheControl.NoCache(null))
    respondTextWriter(contentType = ContentType.Text.EventStream) {
        for (event in events) {
            if (event.id != null) {
                write("id: ${event.id}\n")
            }
            if (event.event != null) {
                write("event: ${event.event}\n")
            }
            for (dataLine in event.data.lines()) {
                write("data: $dataLine\n")
            }
            write("\n")
            flush()
        }
    }
}
