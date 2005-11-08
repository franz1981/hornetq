/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.core.local;

import javax.jms.JMSException;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.PersistenceManager;

/**
 * 
 * Represents a durable topic subscription
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class DurableSubscription extends Subscription
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected String subName;
   
   // Constructors --------------------------------------------------

   public DurableSubscription(String clientID, String subName, Topic topic, String selector,
                              MessageStore ms, PersistenceManager pm)
   {
      super(clientID + "." + subName, topic, selector, ms, pm);
      this.subName = subName;
   }
   

   // Channel implementation ----------------------------------------

   // Public --------------------------------------------------------
   
   public void closeConsumer(PersistenceManager pm) throws JMSException
   {
      //do nothing - this is durable
   }
   
   public String getSubName()
   {
      return subName;
   }
   
   public void load() throws Exception
   {
      this.state.load();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

