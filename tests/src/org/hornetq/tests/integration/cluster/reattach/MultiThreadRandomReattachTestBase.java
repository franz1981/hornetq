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

package org.hornetq.tests.integration.cluster.reattach;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQBytesMessage;
import org.hornetq.jms.client.HornetQTextMessage;

/**
 * A MultiThreadRandomReattachTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 *
 */
public abstract class MultiThreadRandomReattachTestBase extends MultiThreadReattachSupport
{
   private final Logger log = Logger.getLogger(getClass());

   // Constants -----------------------------------------------------

   private static final int RECEIVE_TIMEOUT = 30000;

   private final int LATCH_WAIT = getLatchWait();

   private final int NUM_THREADS = getNumThreads();

   // Attributes ----------------------------------------------------
   protected static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   protected HornetQServer liveServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testA() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestA(sf, threadNum);
         }
      }, NUM_THREADS, false);

   }

   public void testB() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestB(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testC() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestC(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testD() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestD(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testE() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestE(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testF() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestF(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testG() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestG(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testH() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestH(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testI() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestI(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testJ() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestJ(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testK() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestK(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   public void testL() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestL(sf);
         }
      }, NUM_THREADS, true, 10);
   }

   // public void testM() throws Exception
   // {
   // runTestMultipleThreads(new RunnableT()
   // {
   // public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
   // {
   // doTestM(sf, threadNum);
   // }
   // }, NUM_THREADS);
   // }

   public void testN() throws Exception
   {
      runTestMultipleThreads(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestN(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected abstract void start() throws Exception;

   protected abstract void setBody(ClientMessage message) throws Exception;

   protected abstract boolean checkSize(ClientMessage message);

   protected int getNumThreads()
   {
      return 10;
   }

   protected ClientSession createAutoCommitSession(final ClientSessionFactory sf) throws Exception
   {
      return sf.createSession(false, true, true);
   }

   protected ClientSession createTransactionalSession(final ClientSessionFactory sf) throws Exception
   {
      return sf.createSession(false, false, false);
   }

   protected void doTestA(final ClientSessionFactory sf, final int threadNum, final ClientSession session2) throws Exception
   {
      SimpleString subName = new SimpleString("sub" + threadNum);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

      ClientProducer producer = session.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      ClientConsumer consumer = session.createConsumer(subName);

      final int numMessages = 100;

      sendMessages(session, producer, numMessages, threadNum);

      session.start();

      MyHandler handler = new MyHandler(threadNum, numMessages);

      consumer.setMessageHandler(handler);

      boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

      if (!ok)
      {
         throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                             " threadnum " +
                             threadNum);
      }

      if (handler.failure != null)
      {
         throw new Exception("Handler failed: " + handler.failure);
      }

      producer.close();

      consumer.close();

      session.deleteQueue(subName);

      session.close();
   }

   protected void doTestA(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         ClientSession sessConsume = createAutoCommitSession(sf);

         sessConsume.start();

         sessConsume.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);

      }

      ClientSession sessSend = sf.createSession(false, true, true);

      ClientProducer producer = sessSend.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                " threadnum " +
                                threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
         }
      }

      sessSend.close();

      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestB(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         ClientSession sessConsume = createAutoCommitSession(sf);

         sessConsume.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

      ClientProducer producer = sessSend.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.start();
      }

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                " threadnum " +
                                threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
         }
      }

      sessSend.close();

      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

   }

   protected void doTestC(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         ClientSession sessConsume = createTransactionalSession(sf);

         sessConsume.start();

         sessConsume.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);

      ClientProducer producer = sessSend.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                " threadnum " +
                                threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
         }

         handler.reset();
      }

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         Assert.assertTrue(ok);
      }

      for (ClientSession session : sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestD(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + " sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);

         sessConsume.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);

      ClientProducer producer = sessSend.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      for (ClientSession session : sessions)
      {
         session.start();
      }

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                " threadnum " +
                                threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
         }
      }

      handlers.clear();

      // Set handlers to null
      for (ClientConsumer consumer : consumers)
      {
         consumer.setMessageHandler(null);
      }

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      // New handlers
      for (ClientConsumer consumer : consumers)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                " threadnum " +
                                threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed on rollback: " + handler.failure);
         }
      }

      for (ClientSession session : sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + " sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   // Now with synchronous receive()

   protected void doTestE(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true);

         sessConsume.start();

         sessConsume.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

      ClientProducer producer = sessSend.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      consumeMessages(consumers, numMessages, threadNum);

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestF(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true);

         sessConsume.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);

      ClientProducer producer = sessSend.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.start();
      }

      consumeMessages(consumers, numMessages, threadNum);

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestG(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);

         sessConsume.start();

         sessConsume.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);

      ClientProducer producer = sessSend.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestH(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<ClientConsumer>();
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);

         sessConsume.createQueue(MultiThreadRandomReattachTestBase.ADDRESS, subName, null, false);

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);

      ClientProducer producer = sessSend.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      for (ClientSession session : sessions)
      {
         session.start();
      }

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.rollback();
      }

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions)
      {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions)
      {
         session.close();
      }

      for (int i = 0; i < numSessions; i++)
      {
         SimpleString subName = new SimpleString(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));
   }

   protected void doTestI(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true);

      sessCreate.createQueue(MultiThreadRandomReattachTestBase.ADDRESS,
                             new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()),
                             null,
                             false);

      ClientSession sess = sf.createSession(false, true, true);

      sess.start();

      ClientConsumer consumer = sess.createConsumer(new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()));

      ClientProducer producer = sess.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      ClientMessage message = sess.createMessage(HornetQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
      producer.send(message);

      ClientMessage message2 = consumer.receive(MultiThreadRandomReattachTestBase.RECEIVE_TIMEOUT);

      Assert.assertNotNull(message2);

      message2.acknowledge();

      sess.close();

      sessCreate.deleteQueue(new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()));

      sessCreate.close();
   }

   protected void doTestJ(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true);

      sessCreate.createQueue(MultiThreadRandomReattachTestBase.ADDRESS,
                             new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()),
                             null,
                             false);

      ClientSession sess = sf.createSession(false, true, true);

      sess.start();

      ClientConsumer consumer = sess.createConsumer(new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()));

      ClientProducer producer = sess.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      ClientMessage message = sess.createMessage(HornetQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
      producer.send(message);

      ClientMessage message2 = consumer.receive(MultiThreadRandomReattachTestBase.RECEIVE_TIMEOUT);

      Assert.assertNotNull(message2);

      message2.acknowledge();

      sess.close();

      sessCreate.deleteQueue(new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()));

      sessCreate.close();
   }

   protected void doTestK(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      ClientSession s = sf.createSession(false, false, false);

      s.createQueue(MultiThreadRandomReattachTestBase.ADDRESS,
                    new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()),
                    null,
                    false);

      final int numConsumers = 100;

      for (int i = 0; i < numConsumers; i++)
      {
         ClientConsumer consumer = s.createConsumer(new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()));

         consumer.close();
      }

      s.deleteQueue(new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()));

      s.close();
   }

   /*
    * This test tests failure during create connection
    */
   protected void doTestL(final ClientSessionFactory sf) throws Exception
   {
      final int numSessions = 100;

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, false, false);

         session.close();
      }
   }

   protected void doTestN(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      ClientSession sessCreate = sf.createSession(false, true, true);

      sessCreate.createQueue(MultiThreadRandomReattachTestBase.ADDRESS,
                             new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()),
                             null,
                             false);

      ClientSession sess = sf.createSession(false, true, true);

      sess.stop();

      sess.start();

      sess.stop();

      ClientConsumer consumer = sess.createConsumer(new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()));

      ClientProducer producer = sess.createProducer(MultiThreadRandomReattachTestBase.ADDRESS);

      ClientMessage message = sess.createMessage(HornetQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
      producer.send(message);

      sess.start();

      ClientMessage message2 = consumer.receive(MultiThreadRandomReattachTestBase.RECEIVE_TIMEOUT);

      Assert.assertNotNull(message2);

      message2.acknowledge();

      sess.stop();

      sess.start();

      sess.close();

      sessCreate.deleteQueue(new SimpleString(threadNum + MultiThreadRandomReattachTestBase.ADDRESS.toString()));

      sessCreate.close();
   }

   protected int getLatchWait()
   {
      return 60000;
   }

   protected int getNumIterations()
   {
      return 2;
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      log.info("************ Starting test " + getName());
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (liveServer != null && liveServer.isStarted())
      {
         liveServer.stop();
      }

      liveServer = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void runTestMultipleThreads(final RunnableT runnable,
                                       final int numThreads,
                                       final boolean failOnCreateConnection) throws Exception
   {
      runTestMultipleThreads(runnable, numThreads, failOnCreateConnection, 1000);
   }

   private void runTestMultipleThreads(final RunnableT runnable,
                                       final int numThreads,
                                       final boolean failOnCreateConnection,
                                       final long failDelay) throws Exception
   {

      runMultipleThreadsFailoverTest(runnable, numThreads, getNumIterations(), failOnCreateConnection, failDelay);
   }

   /**
    * @return
    */
   @Override
   protected ClientSessionFactoryInternal createSessionFactory()
   {
      final ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) HornetQClient.createClientSessionFactory(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      sf.setReconnectAttempts(-1);
      sf.setConfirmationWindowSize(1024 * 1024);

      return sf;
   }

   @Override
   protected void stop() throws Exception
   {
      liveServer.stop();

      System.gc();

      Assert.assertEquals(0, InVMRegistry.instance.size());
   }

   private void sendMessages(final ClientSession sessSend,
                             final ClientProducer producer,
                             final int numMessages,
                             final int threadNum) throws Exception
   {
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createMessage(HornetQBytesMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("threadnum"), threadNum);
         message.putIntProperty(new SimpleString("count"), i);
         setBody(message);
         producer.send(message);
      }
   }

   private void consumeMessages(final Set<ClientConsumer> consumers, final int numMessages, final int threadNum) throws Exception
   {
      // We make sure the messages arrive in the order they were sent from a particular producer
      Map<ClientConsumer, Map<Integer, Integer>> counts = new HashMap<ClientConsumer, Map<Integer, Integer>>();

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            Map<Integer, Integer> consumerCounts = counts.get(consumer);

            if (consumerCounts == null)
            {
               consumerCounts = new HashMap<Integer, Integer>();
               counts.put(consumer, consumerCounts);
            }

            ClientMessage msg = consumer.receive(MultiThreadRandomReattachTestBase.RECEIVE_TIMEOUT);

            Assert.assertNotNull(msg);

            int tn = (Integer)msg.getObjectProperty(new SimpleString("threadnum"));
            int cnt = (Integer)msg.getObjectProperty(new SimpleString("count"));

            Integer c = consumerCounts.get(tn);
            if (c == null)
            {
               c = new Integer(cnt);
            }

            if (tn == threadNum && cnt != c.intValue())
            {
               throw new Exception("Invalid count, expected " + tn + ": " + c + " got " + cnt);
            }

            c++;

            // Wrap
            if (c == numMessages)
            {
               c = 0;
            }

            consumerCounts.put(tn, c);

            msg.acknowledge();
         }
      }
   }

   // Inner classes -------------------------------------------------

   private class MyHandler implements MessageHandler
   {
      CountDownLatch latch = new CountDownLatch(1);

      private final Map<Integer, Integer> counts = new HashMap<Integer, Integer>();

      volatile String failure;

      final int tn;

      final int numMessages;

      volatile boolean done;

      synchronized void reset()
      {
         counts.clear();

         done = false;

         failure = null;

         latch = new CountDownLatch(1);
      }

      MyHandler(final int threadNum, final int numMessages)
      {
         tn = threadNum;

         this.numMessages = numMessages;
      }

      public synchronized void onMessage(final ClientMessage message)
      {
         try
         {
            message.acknowledge();
         }
         catch (HornetQException me)
         {
            log.error("Failed to process", me);
         }

         if (done)
         {
            return;
         }

         int threadNum = (Integer)message.getObjectProperty(new SimpleString("threadnum"));
         int cnt = (Integer)message.getObjectProperty(new SimpleString("count"));

         Integer c = counts.get(threadNum);
         if (c == null)
         {
            c = new Integer(cnt);
         }

         if (tn == threadNum && cnt != c.intValue())
         {
            failure = "Invalid count, expected " + threadNum + ":" + c + " got " + cnt;
            log.error(failure);

            latch.countDown();
         }

         if (!checkSize(message))
         {
            failure = "Invalid size on message";
            log.error(failure);
            latch.countDown();
         }

         if (tn == threadNum && c == numMessages - 1)
         {
            done = true;
            latch.countDown();
         }

         c++;
         // Wrap around at numMessages
         if (c == numMessages)
         {
            c = 0;
         }

         counts.put(threadNum, c);

      }
   }
}