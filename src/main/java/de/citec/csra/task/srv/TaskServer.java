/* 
 * Copyright (C) 2017 Bielefeld University, Patrick Holthaus
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.citec.csra.task.srv;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import rsb.Event;
import rsb.Factory;
import rsb.Informer;
import rsb.Listener;
import rsb.RSBException;
import rsb.filter.OriginFilter;
import rsb.util.EventQueueAdapter;
import rst.communicationpatterns.TaskStateType.TaskState;
import static rst.communicationpatterns.TaskStateType.TaskState.Origin.SUBMITTER;

/**
 *
 * @author Patrick Holthaus
 * (<a href=mailto:patrick.holthaus@uni-bielefeld.de>patrick.holthaus@uni-bielefeld.de</a>)
 */
public class TaskServer {

	private final static Logger LOG = Logger.getLogger(TaskServer.class.getName());

	private final Listener listener;
	private final Informer informer;
	private final BlockingQueue<Event> queue;
	private final TaskHandler handler;

	public TaskServer(String scope, TaskHandler handler) throws InterruptedException, RSBException {
		EventQueueAdapter qa = new EventQueueAdapter();
		this.handler = handler;
		this.informer = Factory.getInstance().createInformer(scope);
		this.listener = Factory.getInstance().createListener(scope);
		this.listener.addHandler(qa, true);
		this.listener.addFilter(new OriginFilter(this.informer.getId(), true));
		this.queue = qa.getQueue();
	}

	public void listen() throws InterruptedException {
		LOG.log(Level.INFO, "Task server listening at ''{0}''.", this.listener.getScope());
		while (this.listener.isActive()) {
			Event e = this.queue.take();
			LOG.log(Level.INFO, "Received event ''{0}''.", e);
			if (e.getData() instanceof TaskState) {
				TaskState task = (TaskState) e.getData();
				if (task.getOrigin().equals(SUBMITTER)) {
					switch (task.getState()) {
						case INITIATED:
							try {
								handler.handle(task, e, this.informer);
							} catch (RSBException ex) {
								LOG.log(Level.SEVERE, "Could not establish rsb communication, ignoring.", ex);
							} catch (Exception ex) {
								LOG.log(Level.WARNING, "Task at ''{0}'' failed during init ({1}: ''{2}''), ignoring.", new Object[]{e.getScope(), ex, ex.getMessage()});
							}
							break;
						default:
							LOG.log(Level.INFO, "Ignoring event ''{0}''.", e);
							break;
					}
				}
			}
		}
	}

	public void execute() {
		try {
			activate();
			listen();
			deactivate();
		} catch (InterruptedException ex) {
			LOG.log(Level.SEVERE, "Interrupted, shutting down server.", ex);
			Thread.currentThread().interrupt();
		} catch (RSBException ex) {
			LOG.log(Level.SEVERE, "RSB communication failed, shutting down server.", ex);
		}
	}

	public void activate() throws RSBException {
		this.informer.activate();
		this.listener.activate();
	}

	public void deactivate() throws RSBException, InterruptedException {
		this.listener.deactivate();
		this.informer.deactivate();
	}
}
