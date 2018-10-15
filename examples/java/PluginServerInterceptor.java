package com.qlik.sse.basicExample;

import qlik.sse.ServerSideExtension;

import io.grpc.ServerInterceptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Metadata;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import static io.grpc.Metadata.BINARY_BYTE_MARSHALLER;
import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Set;


public class PluginServerInterceptor implements ServerInterceptor {

    private static final Logger logger = Logger.getLogger(PluginServerInterceptor.class.getName());

    private JavaPlugin plugin;
    
    public PluginServerInterceptor(JavaPlugin plugin) {
        this.plugin = plugin;
    }


    @Override
    public <RequestT,ResponseT>ServerCall.Listener<RequestT> interceptCall(
        ServerCall<RequestT,ResponseT> serverCall, final Metadata metadata, ServerCallHandler<RequestT,ResponseT> serverCallHandler) {
        logger.finer("Intercepting call to get metadata.");
        plugin.setMetadata(metadata);
        //logHeader(metadata);
        return serverCallHandler.startCall(new SimpleForwardingServerCall<RequestT,ResponseT>(serverCall){
            @Override
            public void sendHeaders(Metadata responseHeaders) {
                logger.info("Inside Send headers.");
                logHeader(responseHeaders);
               try {
                    ServerSideExtension.FunctionRequestHeader header = ServerSideExtension.FunctionRequestHeader
                    .parseFrom(metadata.get(Metadata.Key.of("qlik-functionrequestheader-bin", BINARY_BYTE_MARSHALLER)));
                    logger.info("Function request header.");
                    logHeader(responseHeaders);
                    if(header.getFunctionId() == 5) {
                        String value = "no-store";
                        responseHeaders.put(Metadata.Key.of("qlik-cache", ASCII_STRING_MARSHALLER),value);
                    } else {
                        String value = "no-store";
                        responseHeaders.remove(Metadata.Key.of("qlik-cache", ASCII_STRING_MARSHALLER),value);
                    }
                } catch(Exception e) {
                   try {
                       ServerSideExtension.ScriptRequestHeader header = ServerSideExtension.ScriptRequestHeader
                               .parseFrom(metadata.get(Metadata.Key.of("qlik-scriptrequestheader-bin", BINARY_BYTE_MARSHALLER)));

                       ServerSideExtension.TableDescription.Builder tableDescriptionBuilder = ServerSideExtension.TableDescription.newBuilder();

                       tableDescriptionBuilder.addFields(0, ServerSideExtension.FieldDescription.newBuilder().setName("ID1").setDataTypeValue(1).build());
                       tableDescriptionBuilder.addFields(1, ServerSideExtension.FieldDescription.newBuilder().setName("ID2").setDataTypeValue(1).build());
                       tableDescriptionBuilder.addFields(2, ServerSideExtension.FieldDescription.newBuilder().setName("ID3").setDataTypeValue(1).build());

                       ServerSideExtension.TableDescription tableDescription = tableDescriptionBuilder.setName("TestTable").build();
                       logger.info(tableDescription.toString());
                       responseHeaders.put(Metadata.Key.of("qlik-tabledescription-bin", ASCII_STRING_MARSHALLER),tableDescription.toString());
                   }catch(Exception ex){
                       logger.fine("Got the script request header.");
                   }

                   logger.info("exception");
               }
                logger.info("send headers call" + responseHeaders);
                super.sendHeaders(responseHeaders);
            }



        }, metadata);
    }
    
    private void logHeader(Metadata header) {
        Set<String> keys = header.keys();
        logger.info("Is header empty? " + keys.isEmpty());
        for(String key : keys) {
            if(key.toLowerCase().contains("-bin")) {
                logger.info("Key: "+ key + " Value: " + header.get(Metadata.Key.of(key, BINARY_BYTE_MARSHALLER)));
            } else {
                logger.info("Key: "+ key + " Value: " + header.get(Metadata.Key.of(key, ASCII_STRING_MARSHALLER)));
            }
            
        }
    }
}