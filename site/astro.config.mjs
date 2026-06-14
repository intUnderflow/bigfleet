// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://bigfleet.lucy.sh",
  integrations: [
    starlight({
      title: "BigFleet",
      description:
        "A fleet-level infrastructure autoscaler. Many clusters, one fleet, one decision engine.",
      logo: { src: "./src/assets/logo.svg", replacesTitle: false },
      social: {
        github: "https://github.com/intUnderflow/bigfleet",
      },
      editLink: {
        baseUrl:
          "https://github.com/intUnderflow/bigfleet/edit/main/site/src/content/docs/",
      },
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Overview", link: "/" },
            { label: "User stories", link: "/user-stories/" },
            { label: "Quickstart", link: "/quickstart/" },
            { label: "Concepts", link: "/concepts/" },
          ],
        },
        {
          label: "Architecture",
          items: [
            { label: "Architecture overview", link: "/architecture/" },
            { label: "API reference", link: "/api-reference/" },
          ],
        },
        {
          label: "Operating",
          items: [
            { label: "Operator guide", link: "/operator-guide/" },
            { label: "Scaling guide", link: "/scaling-guide/" },
            { label: "Scale-test runbook", link: "/scaletest/" },
            { label: "Scale-test results", link: "/scaletest-results/" },
          ],
        },
        {
          label: "Extending",
          items: [
            { label: "Provider author guide", link: "/provider-author-guide/" },
          ],
        },
        {
          label: "Internals",
          collapsed: true,
          items: [
            { label: "Overview & reading order", link: "/internals/readme/" },
            {
              label: "Decision & capacity",
              items: [
                { label: "Decision engine (3 phases)", link: "/internals/decision-engine/" },
                { label: "Phase 1 — OCC assign", link: "/internals/phase1-occ/" },
                { label: "Machine state machine", link: "/internals/machine-lifecycle/" },
                { label: "NeedsTable & aggregation", link: "/internals/needs-table/" },
              ],
            },
            {
              label: "Shard & coordinator",
              items: [
                { label: "Shard hot path", link: "/internals/shard-hot-path/" },
                { label: "Coordinator (Raft)", link: "/internals/coordinator-raft/" },
                { label: "Static stability", link: "/internals/static-stability/" },
              ],
            },
            {
              label: "Protocols & identity",
              items: [
                { label: "Wire protocols & CRDs", link: "/internals/wire-protocols/" },
                { label: "Provider protocol", link: "/internals/provider-protocol/" },
                { label: "Fencing & mTLS identity", link: "/internals/fencing-and-identity/" },
              ],
            },
            {
              label: "Operator & lifecycle",
              items: [
                { label: "Operator & pod controller", link: "/internals/operator-and-controllers/" },
              ],
            },
            {
              label: "Scale & testing",
              items: [
                { label: "Scale-test harness", link: "/internals/scaletest-harness/" },
                { label: "Testing & validation ladder", link: "/internals/testing-and-validation/" },
              ],
            },
            {
              label: "Cross-cutting",
              items: [
                { label: "Data flow (end-to-end)", link: "/internals/data-flow/" },
                { label: "Domain-attribution saga", link: "/internals/domain-attribution/" },
                { label: "Observability & metrics", link: "/internals/observability/" },
              ],
            },
            { label: "ADR decision map", link: "/internals/decision-map/" },
          ],
        },
        {
          label: "Background",
          items: [
            { label: "Implementation plan", link: "/plan/" },
            {
              label: "BigFleet paper (canonical)",
              link: "https://lucy.sh/bigfleet",
              attrs: { target: "_blank", rel: "noopener" },
            },
            {
              label: "Fleet-Scale Kubernetes paper (canonical)",
              link: "https://lucy.sh/fleet-scale-kubernetes",
              attrs: { target: "_blank", rel: "noopener" },
            },
            {
              label: "BigFleet paper (vendored)",
              link: "/papers/bigfleet/",
            },
            {
              label: "Fleet-Scale Kubernetes paper (vendored)",
              link: "/papers/fleet-scale-kubernetes/",
            },
            { label: "ADR index", link: "/adr/" },
          ],
        },
      ],
      customCss: ["./src/styles/custom.css"],
    }),
  ],
});
