#!/usr/bin/env python3
"""
Copyright 2022 Dustin Black

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import re
import unittest
import example_plugin
from arcaflow_plugin_sdk import plugin


class ExamplePluginTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            example_plugin.PodScenarioParams()
        )

        plugin.test_object_serialization(
            example_plugin.PodScenarioResults(
                [
                    example_plugin.Pod(
                        namespace="default",
                        name="nginx-asdf"
                    )
                ]
            )
        )

        plugin.test_object_serialization(
            example_plugin.PodScenarioError(
                error="This is an error"
            )
        )

    def test_functional(self):
        input = example_plugin.PodScenarioParams(
            namespace_pattern=re.compile("foo"),
            pod_name_pattern=re.compile("bar"),
        )

        output_id, output_data = example_plugin.pod_scenario(input)

        # The example plugin always returns an error:
        self.assertEqual("error", output_id)
        self.assertEqual(
            output_data,
            example_plugin.PodScenarioError(
                "Cannot kill pod bar in namespace foo, function not implemented"
            )
        )


if __name__ == '__main__':
    unittest.main()
