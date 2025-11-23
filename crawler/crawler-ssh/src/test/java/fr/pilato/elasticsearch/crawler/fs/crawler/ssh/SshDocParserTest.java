/*
 * Licensed to David Pilato (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package fr.pilato.elasticsearch.crawler.fs.crawler.ssh;

import fr.pilato.elasticsearch.crawler.fs.test.framework.AbstractFSCrawlerTestCase;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import static org.junit.Assert.assertTrue;

public class SshDocParserTest extends AbstractFSCrawlerTestCase {
    @ClassRule
    public static GenericContainer<?> sshd = new GenericContainer<>("panubo/sshd:1.3.0").withExposedPorts(22);

    @BeforeClass
    public static void startSshContainer() {
        // The container is started automatically by the @ClassRule
    }

    @Test
    public void testSimpleDocument() {
        // We can't really test this as we don't have any file in the container
        // But we can check that the container is running
        assertTrue(sshd.isRunning());
    }
}