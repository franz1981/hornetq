/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.client;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.impl.TestSupportPageStore;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.DataConstants;

/**
 * A PagingTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 5, 2008 8:25:58 PM
 *
 *
 */
public class PagingTest extends ServiceTestBase
{

   public PagingTest(final String name)
   {
      super(name);
   }

   public PagingTest()
   {
      super();
   }

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingTest.class);

   private static final int RECEIVE_TIMEOUT = 30000;

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSendReceivePaging() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[numberOfIntegers * 4];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= numberOfIntegers; j++)
         {
            bb.putInt(j);
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.close();

         Assert.assertTrue("TotalMemory expected to be > 0 when it was " + server.getPostOffice()
                                                                                 .getPagingManager()
                                                                                 .getTotalMemory(),
                           server.getPostOffice().getPagingManager().getTotalMemory() > 0);

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         sf = createInVMFactory();

         Assert.assertTrue("TotalMemory expected to be > 0 when it was " + server.getPostOffice()
                                                                                 .getPagingManager()
                                                                                 .getTotalMemory(),
                           server.getPostOffice().getPagingManager().getTotalMemory() > 0);

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            System.out.println("Message " + i + " of " + numberOfMessages);
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            Assert.assertEquals(i, message2.getIntProperty("id").intValue());

            message2.acknowledge();

            Assert.assertNotNull(message2);

            session.commit();

            try
            {
               assertBodiesEqual(body, message2.getBodyBuffer());
            }
            catch (AssertionFailedError e)
            {
               PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
               PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                          .toByteBuffer()
                                                                                          .array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();

         Assert.assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   private void assertBodiesEqual(final byte[] body, final HornetQBuffer buffer)
   {
      byte[] other = new byte[body.length];

      buffer.readBytes(other);

      UnitTestCase.assertEqualsByteArrays(body, other);
   }

   /**
    * - Make a destination in page mode
    * - Add stuff to a transaction
    * - Consume the entire destination (not in page mode any more)
    * - Add stuff to a transaction again
    * - Check order
    * 
    */
   public void testDepageDuringTransaction() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         byte[] body = new byte[DataConstants.SIZE_INT * numberOfIntegers];
         // HornetQBuffer bodyLocal = HornetQChannelBuffers.buffer(DataConstants.SIZE_INT * numberOfIntegers);

         ClientMessage message = null;

         int numberOfMessages = 0;
         while (true)
         {
            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            // Stop sending message as soon as we start paging
            if (server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging())
            {
               break;
            }
            numberOfMessages++;

            producer.send(message);
         }

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());

         session.start();

         ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);

         ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

         for (int i = 0; i < 10; i++)
         {
            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            // Consume messages to force an eventual out of order delivery
            if (i == 5)
            {
               ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
               for (int j = 0; j < numberOfMessages; j++)
               {
                  ClientMessage msg = consumer.receive(PagingTest.RECEIVE_TIMEOUT);
                  msg.acknowledge();
                  Assert.assertNotNull(msg);
               }

               Assert.assertNull(consumer.receiveImmediate());
               consumer.close();
            }

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));
            Assert.assertNotNull(messageID);
            Assert.assertEquals(messageID.intValue(), i);

            producerTransacted.send(message);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         Assert.assertNull(consumer.receiveImmediate());

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 10; i++)
         {
            message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message);

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));

            Assert.assertNotNull(messageID);
            Assert.assertEquals("message received out of order", messageID.intValue(), i);

            message.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         consumer.close();

         session.close();

         Assert.assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testPageOnSchedulingNoRestart() throws Exception
   {
      internalTestPageOnScheduling(false);
   }

   public void testPageOnSchedulingRestart() throws Exception
   {
      internalTestPageOnScheduling(true);
   }

   public void internalTestPageOnScheduling(final boolean restart) throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfMessages = 10000;

      final int numberOfBytes = 1024;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[numberOfBytes];

         for (int j = 0; j < numberOfBytes; j++)
         {
            body[j] = UnitTestCase.getSamplebyte(j);
         }

         long scheduledTime = System.currentTimeMillis() + 5000;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                                     .getPagingManager()
                                                                     .getPageStore(PagingTest.ADDRESS);

            // Worse scenario possible... only schedule what's on pages
            if (store.getCurrentPage() != null)
            {
               message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, scheduledTime);
            }

            producer.send(message);
         }

         if (restart)
         {
            session.close();

            server.stop();

            server = createServer(true,
                                  config,
                                  PagingTest.PAGE_SIZE,
                                  PagingTest.PAGE_MAX,
                                  new HashMap<String, AddressSettings>());
            server.start();

            sf = createInVMFactory();

            session = sf.createSession(null, null, false, true, true, false, 0);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {

            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();

            Assert.assertNotNull(message2);

            Long scheduled = (Long)message2.getObjectProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            if (scheduled != null)
            {
               Assert.assertTrue("Scheduling didn't work", System.currentTimeMillis() >= scheduledTime);
            }

            try
            {
               assertBodiesEqual(body, message2.getBodyBuffer());
            }
            catch (AssertionFailedError e)
            {
               PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
               PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                          .toByteBuffer()
                                                                                          .array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();

         Assert.assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testRollbackOnSend() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, false, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         long initialSize = server.getPostOffice().getPagingManager().getTotalMemory();

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.rollback();

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         Assert.assertNull(consumer.receiveImmediate());

         session.close();

         Assert.assertEquals(initialSize, server.getPostOffice().getPagingManager().getTotalMemory());
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testCommitOnSend() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 10;

      final int numberOfMessages = 10;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         long initialSize = server.getPostOffice().getPagingManager().getTotalMemory();

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.commit();

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();
         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = consumer.receive(500);
            Assert.assertNotNull(msg);
            msg.acknowledge();
         }

         session.commit();

         session.close();

         Assert.assertEquals(initialSize, server.getPostOffice().getPagingManager().getTotalMemory());
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testPageMultipleDestinations() throws Exception
   {
      internalTestPageMultipleDestinations(false);
   }

   public void testPageMultipleDestinationsTransacted() throws Exception
   {
      internalTestPageMultipleDestinations(true);
   }

   public void testDropMessages() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);

      settings.put(PagingTest.ADDRESS.toString(), set);

      HornetQServer server = createServer(true, config, 1024, 10 * 1024, settings);

      server.start();

      final int numberOfMessages = 1000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         Assert.assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());
         Assert.assertEquals(0, server.getPostOffice()
                                      .getPagingManager()
                                      .getPageStore(PagingTest.ADDRESS)
                                      .getAddressSize());

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         session.close();

         session = sf.createSession(false, true, true);

         producer = session.createProducer(PagingTest.ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         session.commit();

         consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         session.commit();

         Assert.assertNull(consumer.receiveImmediate());

         session.close();

         Assert.assertEquals(0, server.getPostOffice().getPagingManager().getTotalMemory());
         Assert.assertEquals(0, server.getPostOffice()
                                      .getPagingManager()
                                      .getPageStore(PagingTest.ADDRESS)
                                      .getAddressSize());

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   private void internalTestPageMultipleDestinations(final boolean transacted) throws Exception
   {
      Configuration config = createDefaultConfig();

      final int NUMBER_OF_BINDINGS = 100;

      int NUMBER_OF_MESSAGES = 2;

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, !transacted, true, false, 0);

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
         {
            session.createQueue(PagingTest.ADDRESS, new SimpleString("someQueue" + i), null, true);
         }

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[1024];

         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            producer.send(message);

            if (transacted)
            {
               session.commit();
            }
         }

         session.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         sf = createInVMFactory();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getTotalMemory() > 0);

         session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         for (int msg = 0; msg < NUMBER_OF_MESSAGES; msg++)
         {

            for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
            {
               ClientConsumer consumer = session.createConsumer(new SimpleString("someQueue" + i));

               ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

               Assert.assertNotNull(message2);

               message2.acknowledge();

               Assert.assertNotNull(message2);

               consumer.close();

            }
         }

         session.close();

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
         {
            Queue queue = (Queue)server.getPostOffice().getBinding(new SimpleString("someQueue" + i)).getBindable();

            Assert.assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getMessageCount());
            Assert.assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getDeliveringCount());
         }

         Assert.assertEquals("There are pending messages on the server", 0, server.getPostOffice()
                                                                                  .getPagingManager()
                                                                                  .getTotalMemory());

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testPagingOneDestinationOnly() throws Exception
   {
      SimpleString PAGED_ADDRESS = new SimpleString("paged");
      SimpleString NON_PAGED_ADDRESS = new SimpleString("non-paged");

      Configuration configuration = createDefaultConfig();

      Map<String, AddressSettings> addresses = new HashMap<String, AddressSettings>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestination = new AddressSettings();
      pagedDestination.setPageSizeBytes(1024);
      pagedDestination.setMaxSizeBytes(10 * 1024);

      addresses.put(PAGED_ADDRESS.toString(), pagedDestination);

      HornetQServer server = createServer(true, configuration, -1, -1, addresses);

      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS, PAGED_ADDRESS, true);

         session.createQueue(NON_PAGED_ADDRESS, NON_PAGED_ADDRESS, true);

         ClientProducer producerPaged = session.createProducer(PAGED_ADDRESS);
         ClientProducer producerNonPaged = session.createProducer(NON_PAGED_ADDRESS);

         int NUMBER_OF_MESSAGES = 100;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerPaged.send(msg);
            producerNonPaged.send(msg);
         }

         session.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS).isPaging());
         Assert.assertFalse(server.getPostOffice().getPagingManager().getPageStore(NON_PAGED_ADDRESS).isPaging());

         session = sf.createSession(false, true, false);

         session.start();

         ClientConsumer consumerNonPaged = session.createConsumer(NON_PAGED_ADDRESS);
         ClientConsumer consumerPaged = session.createConsumer(PAGED_ADDRESS);

         ClientMessage ackList[] = new ClientMessage[NUMBER_OF_MESSAGES];

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerNonPaged.receive(5000);
            Assert.assertNotNull(msg);
            ackList[i] = msg;
         }

         Assert.assertNull(consumerNonPaged.receiveImmediate());

         consumerNonPaged.close();

         for (ClientMessage ack : ackList)
         {
            ack.acknowledge();
         }

         session.commit();

         ackList = null;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerPaged.receive(5000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         Assert.assertNull(consumerPaged.receiveImmediate());

         session.close();

      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testPagingDifferentSizes() throws Exception
   {
      SimpleString PAGED_ADDRESS_A = new SimpleString("paged-a");
      SimpleString PAGED_ADDRESS_B = new SimpleString("paged-b");

      Configuration configuration = createDefaultConfig();

      Map<String, AddressSettings> addresses = new HashMap<String, AddressSettings>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestinationA = new AddressSettings();
      pagedDestinationA.setPageSizeBytes(1024);
      pagedDestinationA.setMaxSizeBytes(10 * 1024);

      int NUMBER_MESSAGES_BEFORE_PAGING = 11;

      addresses.put(PAGED_ADDRESS_A.toString(), pagedDestinationA);

      AddressSettings pagedDestinationB = new AddressSettings();
      pagedDestinationB.setPageSizeBytes(2024);
      pagedDestinationB.setMaxSizeBytes(25 * 1024);

      addresses.put(PAGED_ADDRESS_B.toString(), pagedDestinationB);

      HornetQServer server = createServer(true, configuration, -1, -1, addresses);

      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS_A, PAGED_ADDRESS_A, true);

         session.createQueue(PAGED_ADDRESS_B, PAGED_ADDRESS_B, true);

         ClientProducer producerA = session.createProducer(PAGED_ADDRESS_A);
         ClientProducer producerB = session.createProducer(PAGED_ADDRESS_B);

         int NUMBER_OF_MESSAGES = 100;

         for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertFalse(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = NUMBER_MESSAGES_BEFORE_PAGING * 2; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         ClientConsumer consumerA = session.createConsumer(PAGED_ADDRESS_A);

         ClientConsumer consumerB = session.createConsumer(PAGED_ADDRESS_B);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerA.receive(5000);
            Assert.assertNotNull("Couldn't receive a message on consumerA, iteration = " + i, msg);
            msg.acknowledge();
         }

         Assert.assertNull(consumerA.receiveImmediate());

         consumerA.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerB.receive(5000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         Assert.assertNull(consumerB.receiveImmediate());

         consumerB.close();

         session.close();

      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected Configuration createDefaultConfig()
   {
      Configuration config = super.createDefaultConfig();
      config.setJournalSyncNonTransactional(false);
      return config;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
