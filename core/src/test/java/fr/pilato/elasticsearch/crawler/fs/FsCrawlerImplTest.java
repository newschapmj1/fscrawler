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

package fr.pilato.elasticsearch.crawler.fs;

import fr.pilato.elasticsearch.crawler.fs.settings.FsSettings;
import fr.pilato.elasticsearch.crawler.fs.settings.FsSettingsLoader;
import fr.pilato.elasticsearch.crawler.fs.test.framework.AbstractFSCrawlerTestCase;
import org.junit.Test;

import static fr.pilato.elasticsearch.crawler.fs.FsCrawlerImpl.LOOP_INFINITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/**
 * Unit tests for FsCrawlerImpl.
 * <p>
 * This class currently covers some edge cases in configuration validation
 * that trigger exceptions during FsCrawlerImpl instantiation.
 * <p>
 * Most of the functional testing is done in the integration-tests module.
 * New developers should look at {@link fr.pilato.elasticsearch.crawler.fs.test.integration.elasticsearch.FsCrawlerImplAllDocumentsIT}
 * to understand how the crawler is tested end-to-end.
 */
public class FsCrawlerImplTest extends AbstractFSCrawlerTestCase {

    /**
     * Verifies that the crawler cannot be instantiated with an invalid checksum algorithm.
     */
    @SuppressWarnings("resource")
    @Test
    public void checksum_non_existing_algorithm() {
        FsSettings fsSettings = FsSettingsLoader.load();
        fsSettings.getFs().setChecksum("FSCRAWLER");
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> new FsCrawlerImpl(rootTmpDir, fsSettings, LOOP_INFINITE, false));
    }

    /**
     * Test case for issue #185: <a href="https://github.com/dadoonet/fscrawler/issues/185">https://github.com/dadoonet/fscrawler/issues/185</a> : Add xml_support setting
     * <p>
     * Verifies that the crawler cannot be instantiated with both XML and JSON support enabled simultaneously.
     */
    @SuppressWarnings("resource")
    @Test
    public void xml_and_json_enabled() {
        FsSettings fsSettings = FsSettingsLoader.load();
        fsSettings.getFs().setXmlSupport(true);
        fsSettings.getFs().setJsonSupport(true);
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> new FsCrawlerImpl(rootTmpDir, fsSettings, LOOP_INFINITE, false));
    }

    /**
     * Verifies that the crawler can be successfully instantiated with valid settings.
     */
    @Test
    public void can_instantiate_with_valid_settings() {
        FsSettings fsSettings = FsSettingsLoader.load();
        fsSettings.setName("test-instantiation");

        // We can instantiate it. It creates directories and initializes services.
        // We use try-with-resources to ensure it is closed properly.
        try (FsCrawlerImpl crawler = new FsCrawlerImpl(rootTmpDir, fsSettings, LOOP_INFINITE, false)) {
            assertThat(crawler).isNotNull();
            assertThat(crawler.getManagementService()).isNotNull();
            assertThat(crawler.getDocumentService()).isNotNull();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
