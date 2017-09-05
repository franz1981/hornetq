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

package org.hornetq.tests.performance.journal;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.api.core.HornetQIOErrorException;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * A FakeJournalImplTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FakeJournalImplTest extends JournalImplTestUnit
{
   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      return new FakeSequentialFileFactory();
   }

   @Test(timeout = 10000)
   public void shouldFailOnSlowMoveNextFile() throws Exception
   {
      final CountDownLatch failure = new CountDownLatch(1);
      System.setProperty("journal.timeout", Long.toString(2000));
      final AtomicBoolean slowDown = new AtomicBoolean(false);
      //a factory that will slow down the moveNextFile when necessary
      final FakeSequentialFileFactory fakeSequentialFileFactory = new FakeSequentialFileFactory()
      {

         @Override
         public void activateBuffer(SequentialFile file)
         {
            if (slowDown.get())
            {
               try
               {
                  Assert.assertTrue("Failure hasn't recognized!", failure.await(4, TimeUnit.SECONDS));
               }
               catch (InterruptedException e)
               {
                  Assert.fail("the test can't be interrupted at this point");
               }
            }

            super.activateBuffer(file);
         }

         @Override
         public void onIOError(Exception exception, String message, SequentialFile file)
         {
            if (exception instanceof HornetQIOErrorException)
            {
               //a timeout has happened, just unblock the moveNextFile
               failure.countDown();
            }
         }
      };

      Journal journal = new JournalImpl(10 * 1024 * 1024, 10, 0, 0, fakeSequentialFileFactory, "hornetq-data", "hq", 5000);

      journal.start();

      journal.load(new ArrayList<RecordInfo>(), null, null);

      slowDown.lazySet(true);

      try
      {
         journal.forceMoveNextFile();
      }
      finally
      {

         journal.stop();
      }

   }

}
