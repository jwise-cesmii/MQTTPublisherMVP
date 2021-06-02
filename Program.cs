using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace UAToMQTTGateway
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting UA to MQTT Publisher...");

            // create OPC UA client app
            ApplicationInstance app = new ApplicationInstance
            {
                ApplicationName = "MQTTPublisher",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "Mqtt.Publisher"
            };
            string opcNodeToRead = "ns=2;s=Dynamic/RandomFloat";

            //Mqtt.Publisher.Config.xml
            app.LoadApplicationConfiguration(false).GetAwaiter().GetResult();
            app.CheckApplicationInstanceCertificate(false, 0).GetAwaiter().GetResult();

            // create OPC UA cert validator
            app.ApplicationConfiguration.CertificateValidator = new CertificateValidator();
            app.ApplicationConfiguration.CertificateValidator.CertificateValidation += new CertificateValidationEventHandler(OPCUAServerCertificateValidationCallback);

            // create MQTT client
            //  Using a local network, unsecured Broker. Secure Azure IOT Hub broker connection stuff in comments
            string brokerName = "192.168.1.8";  //<hubname>.azure-devices.net
            string clientName = app.ApplicationName;    //client name is the name of the IoT Hub device you will have to create for this app through the Azure portal
            string sharedKey = "";  //only needed for Azure auth
            string userName = brokerName + "/" + clientName + "/?api-version=2018-06-30";   //extra stuff for Azure auth
            //MqttClient mqttClient = new MqttClient(brokerName, 8883, true, MqttSslProtocols.TLSv1_2, MQTTBrokerCertificateValidationCallback, null);  //Azure connection
            string password = "";
            //password = MakeIoTHubSASToken(brokerName, clientName, sharedKey);
            MqttClient mqttClient = new MqttClient(brokerName);

            // connect to MQTT broker
            byte returnCode = mqttClient.Connect(clientName, userName, password);
            if (returnCode != MqttMsgConnack.CONN_ACCEPTED)
            {
                Console.WriteLine("Connection to MQTT broker failed with " + returnCode.ToString() + "!");
                return;
            }
            else
            {
                Console.WriteLine("Connected to MQTT broker " + brokerName);
            }

            // find endpoint on a local OPC UA server
            string opcServer = "opc.tcp://milo.digitalpetri.com:62541/milo";
            EndpointDescription endpointDescription = CoreClientUtils.SelectEndpoint(opcServer, false);
            EndpointConfiguration endpointConfiguration = EndpointConfiguration.Create(app.ApplicationConfiguration);
            ConfiguredEndpoint endpoint = new ConfiguredEndpoint(null, endpointDescription, endpointConfiguration);

            // Create OPC UA session
            Session session = Session.Create(app.ApplicationConfiguration, endpoint, false, false, app.ApplicationConfiguration.ApplicationName, 30 * 60 * 1000, new UserIdentity(), null).GetAwaiter().GetResult();
            if (!session.Connected)
            {
                Console.WriteLine("Connection to OPC UA server at endpoint {0} failed!", opcServer);
                return;
            }
            else
            {
                Console.WriteLine("Connected to OPC UA server {0}", opcServer);
            }
            Console.WriteLine();

            // send data for a minute, every second
            for (int i = 0; i < 60; i++)
            {
                int publishingInterval = 1000;

                // read a variable node, opcNodeToRead, from the OPC UA server
                DataValue serverTime = session.ReadValue(Variables.Server_ServerStatus_CurrentTime);
                VariableNode node = (VariableNode)session.ReadNode(opcNodeToRead);
                DataValue nodeValue = session.ReadValue(opcNodeToRead);

                // OPC UA PubSub JSON-encode data read
                JsonEncoder encoder = new JsonEncoder(session.MessageContext, true);
                encoder.WriteString("MessageId", i.ToString());
                encoder.WriteString("MessageType", "ua-data");
                encoder.WriteString("PublisherId", app.ApplicationName);
                encoder.PushArray("Messages");
                encoder.PushStructure("");
                encoder.WriteString("DataSetWriterId", endpointDescription.Server.ApplicationUri + ":" + publishingInterval.ToString());
                encoder.PushStructure("Payload");
                encoder.WriteDataValue(node.DisplayName.ToString(), nodeValue);
                encoder.PopStructure();
                encoder.PopStructure();
                encoder.PopArray();
                string payload = encoder.CloseAndReturnText();

                // send to MQTT broker
                Console.WriteLine("Writing OPC UA Node {0} to MQTT Broker with value {1}", opcNodeToRead, nodeValue);
                string topic = "devices/" + clientName + "/messages/events/";
                ushort result = mqttClient.Publish(topic, Encoding.UTF8.GetBytes(payload), MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE, false);

                Task.Delay(publishingInterval).GetAwaiter().GetResult();
            }
            session.Close();
            session.Dispose();
            mqttClient.Disconnect();
        }

        // Used for Azure auth
        private static string MakeIoTHubSASToken(string brokerName, string clientName, string sharedKey)
        {
            // create SAS token
            TimeSpan sinceEpoch = DateTime.UtcNow - new DateTime(1970, 1, 1);
            int week = 60 * 60 * 24 * 7;
            string expiry = Convert.ToString((int)sinceEpoch.TotalSeconds + week);
            string stringToSign = HttpUtility.UrlEncode(brokerName + "/devices/" + clientName) + "\n" + expiry;
            HMACSHA256 hmac = new HMACSHA256(Convert.FromBase64String(sharedKey));
            string signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));

            string password = "SharedAccessSignature sr=" + HttpUtility.UrlEncode(brokerName + "/devices/" + clientName) + "&sig=" + HttpUtility.UrlEncode(signature) + "&se=" + expiry;
            return password;
        }

        private static void OPCUAServerCertificateValidationCallback(CertificateValidator validator, CertificateValidationEventArgs e)
        {
            // always trust the OPC UA server certificate
            if (e.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                e.Accept = true;
            }
        }

        private static bool MQTTBrokerCertificateValidationCallback(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            // always trust the MQTT broker certificate
            return true;
        }
    }
}
